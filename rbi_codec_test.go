package rbi

import (
	"encoding/binary"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	schemapkg "github.com/vapstack/rbi/internal/schema"
	"go.etcd.io/bbolt"
)

type codecRec struct {
	Name string `db:"name" rbi:"index"`
	Age  int    `db:"age" rbi:"index"`
}

type codecSparseRec struct {
	Name string   `db:"name" rbi:"index"`
	Tags []string `db:"tags" rbi:"index"`
	Opt  *string  `db:"opt" rbi:"index"`
}

type codecSparseNameOnlyRec struct {
	Name string `db:"name"`
}

type codecNamedTime time.Time

type codecNamedTimeRec struct {
	When codecNamedTime `db:"when" rbi:"index"`
}

type codecNamedTimeCustom time.Time

func (v codecNamedTimeCustom) EncodeRBI(dst []byte) ([]byte, error) {
	tm := time.Time(v)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(tm.Unix()))
	return binary.LittleEndian.AppendUint32(dst, uint32(tm.Nanosecond())), nil
}

func (v *codecNamedTimeCustom) DecodeRBI(src []byte) error {
	if len(src) != 12 {
		return fmt.Errorf("invalid named time payload")
	}
	tm := time.Unix(int64(binary.LittleEndian.Uint64(src)), int64(binary.LittleEndian.Uint32(src[8:]))).UTC()
	*v = codecNamedTimeCustom(tm)
	return nil
}

func (v codecNamedTimeCustom) IndexingValue() string {
	return time.Time(v).UTC().Format(time.RFC3339Nano)
}

type codecNamedTimeCustomRec struct {
	When codecNamedTimeCustom `db:"when" rbi:"index"`
}

type codecNamedTimeCustomPointerRec struct {
	When  *codecNamedTimeCustom   `db:"when" rbi:"index"`
	Times []*codecNamedTimeCustom `db:"times" rbi:"index"`
}

type codecOpaqueValue struct {
	key  string
	data []byte
}

func (v codecOpaqueValue) EncodeRBI(dst []byte) ([]byte, error) {
	dst = binary.AppendUvarint(dst, uint64(len(v.key)))
	dst = append(dst, v.key...)
	return append(dst, v.data...), nil
}

func (v *codecOpaqueValue) DecodeRBI(src []byte) error {
	length, n := binary.Uvarint(src)
	if n <= 0 || length > uint64(len(src)-n) {
		return fmt.Errorf("invalid opaque value")
	}
	v.key = string(src[n : n+int(length)])
	v.data = append(v.data[:0], src[n+int(length):]...)
	return nil
}

func (v codecOpaqueValue) IndexingValue() string { return v.key }

var _ Codec = (*codecOpaqueValue)(nil)

type codecOpaqueRecord struct {
	Value codecOpaqueValue            `db:"value"`
	Key   codecOpaqueValue            `db:"key" rbi:"unique"`
	Ptr   *codecOpaqueValue           `db:"ptr" rbi:"index"`
	Items []codecOpaqueValue          `db:"items" rbi:"index"`
	Ptrs  []*codecOpaqueValue         `db:"ptrs" rbi:"index"`
	Map   map[string]codecOpaqueValue `db:"map"`
}

var errCodecUser = errors.New("codec user error")

type codecFailValue string

func (v codecFailValue) EncodeRBI(dst []byte) ([]byte, error) {
	dst = append(dst, v...)
	if v == "encode-error" {
		return dst, errCodecUser
	}
	return dst, nil
}

func (v *codecFailValue) DecodeRBI(src []byte) error {
	if string(src) == "decode-error" {
		return errCodecUser
	}
	*v = codecFailValue(src)
	return nil
}

type codecFailRecord struct {
	Value codecFailValue `db:"value"`
}

type codecFailIndexedRecord struct {
	Key   string         `db:"key" rbi:"index"`
	Value codecFailValue `db:"value"`
}

func openTempUint64CollectionCodecRec(t *testing.T, options ...Options) (*Collection[uint64, codecRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_codec.db")
	c, bolt := openBoltAndCollection[uint64, codecRec](t, path, options...)
	t.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})
	return c, path
}

func openTempCollectionUint64CodecSparseRec(t *testing.T, options ...Options) (*Collection[uint64, codecSparseRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_codec_sparse.db")
	c, bolt := openBoltAndCollection[uint64, codecSparseRec](t, path, options...)
	t.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})
	return c, path
}

func codecTestPayload[V any](t *testing.T, rec *V) []byte {
	t.Helper()
	rt, err := schemapkg.Compile(reflect.TypeFor[V](), schemapkg.Config{})
	if err != nil {
		t.Fatalf("Compile(%T): %v", *rec, err)
	}
	payload, err := rt.Codec.Encode(unsafe.Pointer(rec), nil)
	if err != nil {
		t.Fatalf("Encode(%T): %v", *rec, err)
	}
	return slices.Clone(payload)
}

func TestCodec_SetOnChangeSnapshotUsesCompiledRuntime(t *testing.T) {
	c, _ := openTempUint64CollectionCodecRec(t)

	if err := writeSet(c, 1, &codecRec{Name: "alice", Age: 10}, OnChange(func(_ *Tx, _ uint64, oldValue, newValue *codecRec) error {
		if oldValue != nil {
			t.Fatalf("unexpected old value: %#v", oldValue)
		}
		newValue.Age = 11
		return nil
	})); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer c.ReleaseRecords(got)

	if got.Name != "alice" || got.Age != 11 {
		t.Fatalf("unexpected record: %#v", got)
	}

	if err = c.root.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(c.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket missing")
		}
		var key [8]byte
		payload := b.Get(keycodec.U64BytesWithBuf(1, &key))
		if len(payload) == 0 {
			return fmt.Errorf("stored payload missing")
		}
		if payload[0] != 1 {
			return fmt.Errorf("expected compiled codec version byte, got payload %x", payload)
		}
		return nil
	}); err != nil {
		t.Fatalf("Bolt().View: %v", err)
	}
}

func TestCodec_CustomValuePersistenceIndexesAndPatchOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "custom_codec.db")
	c, bolt := openBoltAndCollection[uint64, codecOpaqueRecord](t, path, Options{DisableIndexStore: true})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	src := &codecOpaqueRecord{
		Value: codecOpaqueValue{key: "value", data: []byte("value-data")},
		Key:   codecOpaqueValue{key: "stable", data: []byte("old-private")},
		Ptr:   &codecOpaqueValue{key: "pointer", data: []byte("pointer-data")},
		Items: []codecOpaqueValue{{key: "first", data: []byte("first-data")}, {key: "second", data: []byte("second-data")}},
		Ptrs:  []*codecOpaqueValue{{key: "ptr-item", data: []byte("ptr-item-data")}, nil},
		Map:   map[string]codecOpaqueValue{"mapped": {key: "map-key", data: []byte("map-data")}},
	}
	if err := writeSet(c, 1, src); err != nil {
		t.Fatalf("Set: %v", err)
	}
	src.Value.data[0] = 'X'
	src.Ptr.data[0] = 'X'
	src.Items[0].data[0] = 'X'
	mapped := src.Map["mapped"]
	mapped.data[0] = 'X'
	src.Map["mapped"] = mapped

	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("Get returned nil")
	}
	if string(got.Value.data) != "value-data" || string(got.Ptr.data) != "pointer-data" ||
		string(got.Items[0].data) != "first-data" || string(got.Map["mapped"].data) != "map-data" {
		t.Fatalf("persisted value aliases Set input: %#v", got)
	}

	keys, err := readQueryKeys(c, qx.Query(qx.EQ("key", "stable")))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("unique codec query keys=%v err=%v", keys, err)
	}
	keys, err = readQueryKeys(c, qx.Query(qx.HAS("items", "second")))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("slice codec query keys=%v err=%v", keys, err)
	}
	keys, err = readQueryKeys(c, qx.Query(qx.HASANY("items", []string{"missing", "first"})))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("slice codec HASANY keys=%v err=%v", keys, err)
	}
	keys, err = readQueryKeys(c, qx.Query(qx.HASALL("items", []string{"first", "second"})))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("slice codec HASALL keys=%v err=%v", keys, err)
	}
	keys, err = readQueryKeys(c, qx.Query(qx.GTE("key", "stable")))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("codec range query keys=%v err=%v", keys, err)
	}
	keys, err = readQueryKeys(c, qx.Query(qx.EQ("ptr", "pointer")))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("pointer codec query keys=%v err=%v", keys, err)
	}
	keys, err = readQueryKeys(c, qx.Query(qx.HAS("ptrs", "ptr-item")))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("pointer slice codec query keys=%v err=%v", keys, err)
	}
	conflict := *src
	conflict.Key = codecOpaqueValue{key: "stable", data: []byte("other")}
	if err = writeSet(c, 2, &conflict); err == nil {
		t.Fatal("unique codec accepted duplicate IndexingValue")
	}

	oldValue := *got
	c.ReleaseRecords(got)
	equalPatch, err := c.MakePatch(&oldValue, &oldValue)
	if err != nil || len(equalPatch) != 0 {
		t.Fatalf("equal custom MakePatch=%#v err=%v", equalPatch, err)
	}
	newValue := oldValue
	newValue.Key.data = []byte("new-private")
	patch, err := c.MakePatch(&oldValue, &newValue)
	if err != nil {
		t.Fatalf("MakePatch: %v", err)
	}
	if len(patch) != 1 || patch[0].Name != "key" {
		t.Fatalf("private codec change patch=%#v", patch)
	}
	newValue.Key.data[0] = 'X'
	patchValue := patch[0].Value.(codecOpaqueValue)
	if string(patchValue.data) != "new-private" {
		t.Fatalf("MakePatch value aliases new record: %#v", patchValue)
	}

	tx := BeginUpdate()
	if err = c.Patch(tx, 1, patch); err != nil {
		tx.Release()
		t.Fatalf("Patch: %v", err)
	}
	patchValue.data[0] = 'Y'
	patch[0].Value = patchValue
	if err = tx.Commit(); err != nil {
		tx.Release()
		t.Fatalf("Commit Patch: %v", err)
	}
	tx.Release()

	got, err = readGet(c, 1)
	if err != nil || got == nil {
		t.Fatalf("Get after Patch value=%#v err=%v", got, err)
	}
	if string(got.Key.data) != "new-private" || got.Key.key != "stable" {
		t.Fatalf("Patch queued value aliases caller: %#v", got.Key)
	}
	c.ReleaseRecords(got)

	if err = c.Close(); err != nil {
		t.Fatalf("Close before rebuild: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("bbolt close before rebuild: %v", err)
	}
	c, bolt = openBoltAndCollection[uint64, codecOpaqueRecord](t, path, Options{DisableIndexStore: true})
	keys, err = readQueryKeys(c, qx.Query(qx.EQ("key", "stable")))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("rebuilt unique codec query keys=%v err=%v", keys, err)
	}
	keys, err = readQueryKeys(c, qx.Query(qx.HAS("items", "second")))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("rebuilt slice codec query keys=%v err=%v", keys, err)
	}
	keys, err = readQueryKeys(c, qx.Query(qx.HAS("ptrs", "ptr-item")))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("rebuilt pointer slice codec query keys=%v err=%v", keys, err)
	}
}

func TestCodec_MakePatchSamePointerReturnsEmptyAndResetsDst(t *testing.T) {
	c := openTempUint64CollectionReflect[codecFailRecord](t, "custom_codec_same_pointer.db")
	value := &codecFailRecord{Value: "encode-error"}
	dst := []Field{{Name: "stale", Value: "value"}}

	patch, err := c.MakePatchInto(value, value, dst)
	if err != nil {
		t.Fatalf("MakePatchInto same pointer: %v", err)
	}
	if len(patch) != 0 {
		t.Fatalf("MakePatchInto same pointer patch=%#v, want empty", patch)
	}
}

func TestCodec_CustomErrorsPreserveWriteAtomicity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "custom_codec_errors.db")
	c, bolt := openBoltAndCollection[uint64, codecFailRecord](t, path)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	tx := BeginUpdate()
	err := c.Set(tx, 1, &codecFailRecord{Value: "encode-error"})
	if !errors.Is(err, errCodecUser) {
		tx.Release()
		t.Fatalf("Set encode err=%v", err)
	}
	if err = tx.Commit(); err != nil {
		tx.Release()
		t.Fatalf("empty commit after Set error: %v", err)
	}
	tx.Release()
	got, err := readGet(c, 1)
	if err != nil || got != nil {
		t.Fatalf("failed Set persisted value=%#v err=%v", got, err)
	}

	if err = writeSet(c, 1, &codecFailRecord{Value: "old"}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	tx = BeginUpdate()
	err = c.Set(tx, 2, &codecFailRecord{Value: "decode-error"}, OnChange(func(_ *Tx, _ uint64, _, _ *codecFailRecord) error { return nil }))
	if !errors.Is(err, errCodecUser) {
		tx.Release()
		t.Fatalf("Set clone err=%v", err)
	}
	if err = tx.Commit(); err != nil {
		tx.Release()
		t.Fatalf("empty commit after clone error: %v", err)
	}
	tx.Release()
	err = writeSet(c, 1, &codecFailRecord{Value: "new"}, OnChange(func(_ *Tx, _ uint64, _, newValue *codecFailRecord) error {
		newValue.Value = "encode-error"
		return nil
	}))
	if !errors.Is(err, errCodecUser) {
		t.Fatalf("OnChange encode err=%v", err)
	}
	got, err = readGet(c, 1)
	if err != nil || got == nil || got.Value != "old" {
		t.Fatalf("OnChange encode error changed record=%#v err=%v", got, err)
	}
	if got != nil {
		c.ReleaseRecords(got)
	}

	tx = BeginUpdate()
	err = c.Patch(tx, 1, []Field{{Name: "value", Value: codecFailValue("encode-error")}})
	if !errors.Is(err, errCodecUser) {
		tx.Release()
		t.Fatalf("Patch encode err=%v", err)
	}
	tx.Release()
	tx = BeginUpdate()
	err = c.Patch(tx, 1, []Field{{Name: "value", Value: codecFailValue("decode-error")}})
	if !errors.Is(err, errCodecUser) {
		tx.Release()
		t.Fatalf("Patch decode err=%v", err)
	}
	tx.Release()

	tx = BeginUpdate()
	if err = c.Patch(tx, 1, []Field{{Name: "value", Value: codecFailValue("patched")}}, OnChange(func(_ *Tx, _ uint64, _, newValue *codecFailRecord) error {
		newValue.Value = "encode-error"
		return nil
	})); err != nil {
		tx.Release()
		t.Fatalf("queue Patch: %v", err)
	}
	err = tx.Commit()
	tx.Release()
	if !errors.Is(err, errCodecUser) {
		t.Fatalf("Patch commit encode err=%v", err)
	}
	got, err = readGet(c, 1)
	if err != nil || got == nil || got.Value != "old" {
		t.Fatalf("Patch encode error changed record=%#v err=%v", got, err)
	}
	if got != nil {
		c.ReleaseRecords(got)
	}
}

func TestCodec_CustomDecodeErrorStopsIndexRebuild(t *testing.T) {
	path := filepath.Join(t.TempDir(), "custom_codec_rebuild_error.db")
	c, bolt := openBoltAndCollection[uint64, codecFailIndexedRecord](t, path, Options{Index: map[string]IndexKind{}})
	if err := writeSet(c, 1, &codecFailIndexedRecord{Key: "key", Value: "decode-error"}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close seed collection: %v", err)
	}
	if err := bolt.Close(); err != nil {
		t.Fatalf("Close seed bbolt: %v", err)
	}

	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen bbolt: %v", err)
	}
	defer func() { _ = bolt.Close() }()
	c, err = Open[uint64, codecFailIndexedRecord](bolt, testOptions(Options{}))
	if err == nil {
		_ = c.Close()
		t.Fatal("Open accepted custom decode error during rebuild")
	}
	if !errors.Is(err, errCodecUser) || !strings.Contains(err.Error(), "stage=decode") || !strings.Contains(err.Error(), "scan_pos=1") {
		t.Fatalf("Open rebuild decode err=%v", err)
	}
}

func TestCodec_CustomPatchEncodeErrorDoesNotChangeIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "custom_codec_patch_index_error.db")
	c, bolt := openBoltAndCollection[uint64, codecFailIndexedRecord](t, path)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	if err := writeSet(c, 1, &codecFailIndexedRecord{Key: "old-key", Value: "old"}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	tx := BeginUpdate()
	if err := c.Patch(tx, 1, []Field{{Name: "value", Value: codecFailValue("patched")}}, OnChange(func(_ *Tx, _ uint64, _, newValue *codecFailIndexedRecord) error {
		newValue.Key = "new-key"
		newValue.Value = "encode-error"
		return nil
	})); err != nil {
		tx.Release()
		t.Fatalf("queue Patch: %v", err)
	}
	err := tx.Commit()
	tx.Release()
	if !errors.Is(err, errCodecUser) {
		t.Fatalf("Patch commit err=%v", err)
	}
	oldKeys, err := readQueryKeys(c, qx.Query(qx.EQ("key", "old-key")))
	if err != nil || !slices.Equal(oldKeys, []uint64{1}) {
		t.Fatalf("old index keys=%v err=%v", oldKeys, err)
	}
	newKeys, err := readQueryKeys(c, qx.Query(qx.EQ("key", "new-key")))
	if err != nil || len(newKeys) != 0 {
		t.Fatalf("new index keys=%v err=%v", newKeys, err)
	}
	got, err := readGet(c, 1)
	if err != nil || got == nil || got.Key != "old-key" || got.Value != "old" {
		t.Fatalf("record after failed Patch=%#v err=%v", got, err)
	}
	if got != nil {
		c.ReleaseRecords(got)
	}
}

func BenchmarkCodec_CustomValueIndexerWrite(b *testing.B) {
	type record struct {
		Value codecOpaqueValue `db:"value" rbi:"index"`
	}
	path := filepath.Join(b.TempDir(), "custom_codec_write_bench.db")
	c, bolt := openBoltAndCollection[uint64, record](b, path, Options{DisableIndexStore: true})
	c.disableSync()
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	value := &record{Value: codecOpaqueValue{key: "key", data: []byte("payload")}}
	b.Run("Set", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := writeSet(c, 1, value); err != nil {
				b.Fatal(err)
			}
		}
	})
	if err := writeSet(c, 1, value); err != nil {
		b.Fatal(err)
	}
	patch := []Field{{Name: "value", Value: codecOpaqueValue{key: "key", data: []byte("patched")}}}
	b.Run("Patch", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := writePatch(c, 1, patch); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestCodec_RejectsNamedTimeIndexField(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	c, err := Open[uint64, codecNamedTimeRec](raw, testOptions(Options{}))
	if err == nil {
		_ = c.Close()
		t.Fatal("New accepted named time index field")
	}
	if msg := err.Error(); !strings.Contains(msg, "named time field") || !strings.Contains(msg, "is not supported") {
		t.Fatalf("New named time index field err=%v", err)
	}
}

func TestCodec_NamedTimeWithCustomCodecAndValueIndexer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "named_time_custom.db")
	c, bolt := openBoltAndCollection[uint64, codecNamedTimeCustomRec](t, path)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	when := codecNamedTimeCustom(time.Date(2026, time.July, 11, 10, 20, 30, 456, time.UTC))
	if err := writeSet(c, 1, &codecNamedTimeCustomRec{When: when}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	keys, err := readQueryKeys(c, qx.Query(qx.EQ("when", when)))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("Query named time codec keys=%v err=%v", keys, err)
	}
	got, err := readGet(c, 1)
	if err != nil || got == nil || !time.Time(got.When).Equal(time.Time(when)) {
		t.Fatalf("Get named time codec=%#v err=%v", got, err)
	}
	if got != nil {
		c.ReleaseRecords(got)
	}
}

func TestCodec_NamedTimeCodecBehindPointers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "named_time_pointer_custom.db")
	c, bolt := openBoltAndCollection[uint64, codecNamedTimeCustomPointerRec](t, path)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	when := codecNamedTimeCustom(time.Date(2026, time.July, 12, 10, 20, 30, 456, time.UTC))
	other := codecNamedTimeCustom(time.Date(2026, time.July, 13, 11, 21, 31, 789, time.UTC))
	want := &codecNamedTimeCustomPointerRec{
		When:  &when,
		Times: []*codecNamedTimeCustom{&when, nil, &other},
	}
	if err := writeSet(c, 1, want); err != nil {
		t.Fatalf("Set: %v", err)
	}

	keys, err := readQueryKeys(c, qx.Query(qx.EQ("when", when.IndexingValue())))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("Query pointer named time codec keys=%v err=%v", keys, err)
	}
	keys, err = readQueryKeys(c, qx.Query(qx.HAS("times", other.IndexingValue())))
	if err != nil || !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("Query pointer slice named time codec keys=%v err=%v", keys, err)
	}

	got, err := readGet(c, 1)
	if err != nil || got == nil || got.When == nil || !time.Time(*got.When).Equal(time.Time(when)) ||
		len(got.Times) != 3 || got.Times[0] == nil || !time.Time(*got.Times[0]).Equal(time.Time(when)) ||
		got.Times[1] != nil || got.Times[2] == nil || !time.Time(*got.Times[2]).Equal(time.Time(other)) {
		t.Fatalf("Get pointer named time codec=%#v err=%v", got, err)
	}
	if got != nil {
		c.ReleaseRecords(got)
	}
}

func TestCodec_BuildIndexUsesCompiledRuntimeDecode(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	const bucket = "codec_bucket"
	payload := codecTestPayload(t, &codecRec{Name: "alice", Age: 10})

	if err := raw.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		var key [8]byte
		return b.Put(keycodec.U64BytesWithBuf(1, &key), payload)
	}); err != nil {
		t.Fatalf("seed payload: %v", err)
	}

	c, err := Open[uint64, codecRec](raw, testOptions(Options{BucketName: bucket}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Close() }()

	got, err := readQueryKeys(c, qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("unexpected query result after reopen: %v", got)
	}

	rec, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if rec == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer c.ReleaseRecords(rec)

	if rec.Name != "alice" || rec.Age != 10 {
		t.Fatalf("unexpected record after reopen: %#v", rec)
	}
}

func TestCodec_BuildIndexRejectsOldPayloadFormat(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	const bucket = "codec_old_payload"
	if err := raw.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		var key [8]byte
		return b.Put(keycodec.U64BytesWithBuf(1, &key), []byte("RBI1-old"))
	}); err != nil {
		t.Fatalf("seed old payload: %v", err)
	}

	c, err := Open[uint64, codecRec](raw, testOptions(Options{BucketName: bucket}))
	if err == nil {
		_ = c.Close()
		t.Fatal("Open accepted old payload format")
	}
	if !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("Open err=%v want unsupported version", err)
	}
}

func TestCodec_RecordPoolReusedDecodeZerosMissingFields(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	const bucket = "codec_sparse"
	opt := "sticky"
	first := codecTestPayload(t, &codecSparseRec{
		Name: "first",
		Tags: []string{"go", "db"},
		Opt:  &opt,
	})
	second := codecTestPayload(t, &codecSparseNameOnlyRec{Name: "second"})

	if err := raw.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		var key [8]byte
		if err = b.Put(keycodec.U64BytesWithBuf(1, &key), first); err != nil {
			return err
		}
		return b.Put(keycodec.U64BytesWithBuf(2, &key), second)
	}); err != nil {
		t.Fatalf("seed sparse payloads: %v", err)
	}

	c, err := Open[uint64, codecSparseRec](raw, testOptions(Options{BucketName: bucket, BatchSoftLimit: 1}))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = c.Close() }()

	const attempts = 256
	for attempt := 0; attempt < attempts; attempt++ {
		first, err := readGet(c, 1)
		if err != nil {
			t.Fatalf("Get(1) attempt=%d: %v", attempt, err)
		}
		ptr1 := uintptr(unsafe.Pointer(first))
		if first.Name != "first" || !slices.Equal(first.Tags, []string{"go", "db"}) || first.Opt == nil || *first.Opt != opt {
			t.Fatalf("unexpected first record before release: %#v", first)
		}
		c.ReleaseRecords(first)

		second, err := readGet(c, 2)
		if err != nil {
			t.Fatalf("Get(2) attempt=%d: %v", attempt, err)
		}
		ptr2 := uintptr(unsafe.Pointer(second))
		if ptr1 != ptr2 {
			c.ReleaseRecords(second)
			continue
		}
		if second.Name != "second" {
			t.Fatalf("pooled decode returned wrong name: %#v", second)
		}
		if len(second.Tags) != 0 || second.Opt != nil {
			t.Fatalf("pooled decode leaked fields from previous record: %#v", second)
		}
		c.ReleaseRecords(second)
		return
	}

	if testRaceEnabled {
		t.Skipf("sync.Pool reuse is not guaranteed under -race after %d attempts", attempts)
	}
	t.Fatalf("failed to observe record pool reuse after %d attempts", attempts)
}

func TestCodec_RecordPoolReleaseZeroesRecord(t *testing.T) {
	c, _ := openTempCollectionUint64CodecSparseRec(t)

	opt := "sticky"
	rec := c.recPool.Get()
	rec.Name = "dirty"
	rec.Tags = []string{"go", "db"}
	rec.Opt = &opt

	c.ReleaseRecords(rec)
	if rec.Name != "" || rec.Tags != nil || rec.Opt != nil {
		t.Fatalf("released record was not zeroed: %#v", rec)
	}
}

const postingImportPath = "github.com/vapstack/rbi/internal/posting"

func TestPostingListProductionValueOnlyGuard(t *testing.T) {
	root := testRepoRoot(t)
	fset := token.NewFileSet()
	var failures []string

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if strings.HasPrefix(name, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return parseErr
		}

		aliases := postingImportAliases(file)
		inPostingPackage := strings.Contains(path, string(filepath.Separator)+"internal"+string(filepath.Separator)+"posting"+string(filepath.Separator))
		fileFailures := checkPostingListValueOnlyFile(fset, file, aliases, inPostingPackage)
		failures = append(failures, fileFailures...)
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}
	if len(failures) > 0 {
		t.Fatalf("posting.List production guard failed:\n%s", strings.Join(failures, "\n"))
	}
}

func testRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

func postingImportAliases(file *ast.File) map[string]struct{} {
	aliases := make(map[string]struct{})
	for _, imp := range file.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		if path != postingImportPath {
			continue
		}
		name := "posting"
		if imp.Name != nil {
			name = imp.Name.Name
		}
		aliases[name] = struct{}{}
	}
	return aliases
}

func checkPostingListValueOnlyFile(
	fset *token.FileSet,
	file *ast.File,
	postingAliases map[string]struct{},
	inPostingPackage bool,
) []string {
	var failures []string

	report := func(pos token.Pos, msg string) {
		failures = append(failures, fset.Position(pos).String()+": "+msg)
	}

	var checkFieldList func(fl *ast.FieldList)
	var checkTypeExpr func(expr ast.Expr)

	checkFieldList = func(fl *ast.FieldList) {
		if fl == nil {
			return
		}
		for _, field := range fl.List {
			checkTypeExpr(field.Type)
		}
	}

	checkTypeExpr = func(expr ast.Expr) {
		switch v := expr.(type) {
		case nil:
			return
		case *ast.StarExpr:
			if isPostingListType(v.X, postingAliases, inPostingPackage) {
				report(v.Pos(), "production code must not use *posting.List or *List")
			}
			checkTypeExpr(v.X)
		case *ast.ArrayType:
			checkTypeExpr(v.Elt)
		case *ast.Ellipsis:
			checkTypeExpr(v.Elt)
		case *ast.MapType:
			checkTypeExpr(v.Key)
			checkTypeExpr(v.Value)
		case *ast.ChanType:
			checkTypeExpr(v.Value)
		case *ast.StructType:
			checkFieldList(v.Fields)
		case *ast.InterfaceType:
			checkFieldList(v.Methods)
		case *ast.FuncType:
			checkFieldList(v.Params)
			checkFieldList(v.Results)
		case *ast.ParenExpr:
			checkTypeExpr(v.X)
		case *ast.IndexExpr:
			checkTypeExpr(v.X)
			checkTypeExpr(v.Index)
		case *ast.IndexListExpr:
			checkTypeExpr(v.X)
			for _, idx := range v.Indices {
				checkTypeExpr(idx)
			}
		}
	}

	for _, decl := range file.Decls {
		switch v := decl.(type) {
		case *ast.FuncDecl:
			checkFieldList(v.Recv)
			checkTypeExpr(v.Type)
		case *ast.GenDecl:
			for _, spec := range v.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					checkTypeExpr(s.Type)
				case *ast.ValueSpec:
					checkTypeExpr(s.Type)
				}
			}
		}
	}

	return failures
}

func isPostingListType(expr ast.Expr, postingAliases map[string]struct{}, inPostingPackage bool) bool {
	switch v := expr.(type) {
	case *ast.Ident:
		return inPostingPackage && v.Name == "List"
	case *ast.SelectorExpr:
		pkg, ok := v.X.(*ast.Ident)
		if !ok || v.Sel.Name != "List" {
			return false
		}
		_, ok = postingAliases[pkg.Name]
		return ok
	default:
		return false
	}
}
