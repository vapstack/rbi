package rbi

import (
	"encoding/hex"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
)

type reflectFoldedString string

func (s reflectFoldedString) IndexingValue() string {
	return strings.ToLower(string(s))
}

type reflectOrdinalVI int

func (v reflectOrdinalVI) IndexingValue() string {
	return string(rune('a' + v))
}

type reflectReverseOrdinalVI int

func (v reflectReverseOrdinalVI) IndexingValue() string {
	return string(rune('z' - v))
}

type reflectScalarVIRec struct {
	Code reflectFoldedString `db:"code" rbi:"index"`
}

type reflectOrdinalVIRec struct {
	Code reflectOrdinalVI `db:"code" rbi:"index"`
}

type reflectScalarVIUniqueRec struct {
	Code reflectFoldedString `db:"code" rbi:"unique"`
}

type reflectSliceVIRec struct {
	Tags []reflectFoldedString `db:"tags" rbi:"index"`
}

type reflectHexBytes []byte

func (b reflectHexBytes) IndexingValue() string {
	return hex.EncodeToString([]byte(b))
}

type reflectPtrHexBytes []byte

func (b *reflectPtrHexBytes) IndexingValue() string {
	if b == nil {
		return ""
	}
	return hex.EncodeToString([]byte(*b))
}

type reflectBytesVIRec struct {
	Key reflectHexBytes `db:"key" rbi:"index"`
}

type reflectPtrBytesVIRec struct {
	Key *reflectPtrHexBytes `db:"key" rbi:"index"`
}

type reflectMapVI map[string]string

func (m reflectMapVI) IndexingValue() string {
	return strings.ToLower(m["id"])
}

type reflectPtrWordVI struct {
	Ptr *string
}

func (v reflectPtrWordVI) IndexingValue() string {
	if v.Ptr == nil {
		return "<nil>"
	}
	return strings.ToLower(*v.Ptr)
}

type reflectMapVIRec struct {
	Key reflectMapVI `db:"key" rbi:"unique"`
}

type reflectPtrWordVIRec struct {
	Key reflectPtrWordVI `db:"key" rbi:"unique"`
}

type reflectInterfaceVIRec struct {
	Key ValueIndexer `db:"key" rbi:"unique"`
}

type reflectInterfaceVISliceRec struct {
	Tags []ValueIndexer `db:"tags" rbi:"index"`
}

type reflectPtrFoldedString string

func (s *reflectPtrFoldedString) IndexingValue() string {
	if s == nil {
		return "<nil>"
	}
	return strings.ToLower(string(*s))
}

type ReflectUnsafeEmbeddedIndexed struct {
	Code  *reflectPtrFoldedString `db:"code" rbi:"unique"`
	Score int                     `db:"score" rbi:"index"`
	Tags  []string                `db:"tags" rbi:"index"`
	Count *uint64                 `db:"count" rbi:"index"`
}

type reflectUnsafeAccessorRec struct {
	Name string `db:"name" rbi:"index"`
	ReflectUnsafeEmbeddedIndexed
}

type reflectPatchTimeRec struct {
	Name    string `db:"name" rbi:"index"`
	When    time.Time
	Slots   []time.Time
	Windows map[string]time.Time
}

type reflectNamedTime time.Time
type reflectNamedTimePtr *time.Time

type reflectNamedTimeRec struct {
	When reflectNamedTime `db:"when" rbi:"index"`
}

type reflectTimeRec struct {
	When time.Time `db:"when" rbi:"index"`
}

type reflectTimePtrRec struct {
	When *time.Time `db:"when" rbi:"index"`
}

type reflectNamedTimePointerRec struct {
	When *reflectNamedTime `db:"when" rbi:"index"`
}

type reflectNamedTimePtrRec struct {
	When reflectNamedTimePtr `db:"when" rbi:"index"`
}

type reflectTimeVI time.Time

func (v reflectTimeVI) IndexingValue() string {
	return time.Time(v).UTC().Format(time.RFC3339Nano)
}

type reflectTimeVIRec struct {
	When reflectTimeVI `db:"when" rbi:"unique"`
}

type reflectPtrTimeVI time.Time

func (v *reflectPtrTimeVI) IndexingValue() string {
	if v == nil {
		return "<nil>"
	}
	return time.Time(*v).UTC().Format(time.RFC3339Nano)
}

type reflectPtrTimeVIRec struct {
	When *reflectPtrTimeVI `db:"when" rbi:"unique"`
}

type reflectInt64AgeRec struct {
	Age int64 `db:"age" rbi:"index"`
}

type reflectNamedTag string
type reflectNamedTags []reflectNamedTag

type reflectNamedSlicePatchRec struct {
	Name string           `db:"name" rbi:"index"`
	Tags reflectNamedTags `db:"tags" rbi:"index"`
}

type reflectPatchNestedChild struct {
	Label  string
	Values []int
}

type reflectPatchNested struct {
	Name  string
	Tags  []string
	Attrs map[string]int
	Child *reflectPatchNestedChild
}

type reflectPatchNestedRec struct {
	Name      string `db:"name" rbi:"index"`
	Nested    reflectPatchNested
	NestedPtr *reflectPatchNested
}

type reflectPatchNestedSliceRec struct {
	Name  string `db:"name" rbi:"index"`
	Items []reflectPatchNestedRec
}

type reflectNumericPatchRec struct {
	Name  string `db:"name" rbi:"index"`
	I8    int8
	U8    uint8
	I64   int64
	U64   uint64
	Bytes []uint8
}

func openTempUint64CollectionReflect[V any](t *testing.T, filename string, options ...Options) *Collection[uint64, V] {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, filename)
	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	opts = testOptions(opts)

	c, err := Open[uint64, V](bolt, opts)
	if err != nil {
		_ = bolt.Close()
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})

	return c
}

func assertNewRejectsNamedTime[V any](t *testing.T) {
	t.Helper()
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	c, err := Open[uint64, V](raw, testOptions(Options{}))
	if err == nil {
		_ = c.Close()
		t.Fatalf("New accepted indexed named time type %v", reflect.TypeFor[V]())
	}
	if msg := err.Error(); !strings.Contains(msg, "named time field") || !strings.Contains(msg, "is not supported") {
		t.Fatalf("New indexed named time type err=%v", err)
	}
}

func patchFieldsByName(fields []Field) map[string]any {
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		out[f.Name] = f.Value
	}
	return out
}

func applyPatchForTest[V any](t testing.TB, c *Collection[uint64, V], old *V, patch []Field) *V {
	t.Helper()
	if err := writeSet(c, 1, old); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := writePatch(c, 1, patch, PatchStrict); err != nil {
		t.Fatalf("Patch: %v", err)
	}
	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatalf("Get returned nil after Patch")
	}
	return got
}

func assertUint64Slice(t *testing.T, got, want []uint64) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected ids: got=%v want=%v", got, want)
	}
}

func assertUint64Set(t *testing.T, got, want []uint64) {
	t.Helper()

	got = append([]uint64(nil), got...)
	want = append([]uint64(nil), want...)
	slices.Sort(got)
	slices.Sort(want)

	assertUint64Slice(t, got, want)
}

func TestReflectExt_QueryValueIndexerScalarNamedString_NormalizesQueryValue(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectScalarVIRec](t, "reflect_scalar_vi.db")

	if err := writeSets(c,
		[]uint64{1, 2},
		[]*reflectScalarVIRec{
			{Code: reflectFoldedString("MiXeD")},
			{Code: reflectFoldedString("mixed")},
		},
	); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	q := qx.Query(qx.EQ("code", reflectFoldedString("MiXeD")))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Set(t, got, []uint64{1, 2})

	cnt, err := readCount(c, q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("Count mismatch: got=%d want=2", cnt)
	}
}

func TestReflectExt_QueryValueIndexerScalarPlainString_UsesCanonicalKey(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectScalarVIRec](t, "reflect_scalar_vi_plain.db")

	if err := writeSets(c,
		[]uint64{1, 2},
		[]*reflectScalarVIRec{
			{Code: reflectFoldedString("MiXeD")},
			{Code: reflectFoldedString("mixed")},
		},
	); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	q := qx.Query(qx.EQ("code", "mixed"))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Set(t, got, []uint64{1, 2})
}

func TestReflectExt_QueryValueIndexerScalarMixedTypedAndPlainStringBounds(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectScalarVIRec](t, "reflect_scalar_vi_mixed_bounds.db")

	if err := writeSets(c,
		[]uint64{1, 2, 3},
		[]*reflectScalarVIRec{
			{Code: reflectFoldedString("AA")},
			{Code: reflectFoldedString("aa")},
			{Code: reflectFoldedString("zz")},
		},
	); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	q := qx.Query(qx.AND(
		qx.LTE("code", reflectFoldedString("AA")),
		qx.NOT(qx.LTE("code", "AA")),
	))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Set(t, got, []uint64{1, 2})
}

func TestReflectExt_QueryValueIndexerScalarMixedValueIndexerTypesBounds(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectOrdinalVIRec](t, "reflect_ordinal_vi_mixed_bounds.db")

	if err := writeSets(c,
		[]uint64{1, 2, 3},
		[]*reflectOrdinalVIRec{
			{Code: reflectOrdinalVI(1)},
			{Code: reflectOrdinalVI(2)},
			{Code: reflectOrdinalVI(25)},
		},
	); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	q := qx.Query(qx.AND(
		qx.LTE("code", reflectReverseOrdinalVI(1)),
		qx.NOT(qx.LTE("code", reflectOrdinalVI(1))),
	))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Set(t, got, []uint64{2})
}

func TestReflectExt_UniqueValueIndexerScalarNamedString_UsesIndexingValue(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectScalarVIUniqueRec](t, "reflect_scalar_vi_unique.db")

	if err := writeSet(c, 1, &reflectScalarVIUniqueRec{Code: reflectFoldedString("MiXeD")}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	err := writeSet(c, 2, &reflectScalarVIUniqueRec{Code: reflectFoldedString("mixed")})
	if err == nil {
		t.Fatalf("expected unique violation for equal IndexingValue()")
	}
	if !errors.Is(err, rbierrors.ErrUniqueViolation) && !strings.Contains(err.Error(), rbierrors.ErrUniqueViolation.Error()) {
		t.Fatalf("expected rbierrors.ErrUniqueViolation-compatible error, got: %v", err)
	}
}

func TestReflectExt_QueryValueIndexerSliceNamedString_NormalizesElements(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectSliceVIRec](t, "reflect_slice_vi.db")

	if err := writeSets(c,
		[]uint64{1, 2},
		[]*reflectSliceVIRec{
			{Tags: []reflectFoldedString{reflectFoldedString("MiXeD")}},
			{Tags: []reflectFoldedString{reflectFoldedString("mixed")}},
		},
	); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	q := qx.Query(qx.HASANY("tags", []reflectFoldedString{reflectFoldedString("mixed")}))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Set(t, got, []uint64{1, 2})

	cnt, err := readCount(c, q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("Count mismatch: got=%d want=2", cnt)
	}
}

func TestReflectExt_QueryValueIndexerSlicePlainStrings_UseCanonicalKeys(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectSliceVIRec](t, "reflect_slice_vi_plain.db")

	if err := writeSets(c,
		[]uint64{1, 2},
		[]*reflectSliceVIRec{
			{Tags: []reflectFoldedString{reflectFoldedString("MiXeD")}},
			{Tags: []reflectFoldedString{reflectFoldedString("mixed")}},
		},
	); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	q := qx.Query(qx.HASANY("tags", []string{"mixed"}))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Set(t, got, []uint64{1, 2})
}

func TestReflectExt_QueryValueIndexerScalar_POSSort_TypedPrioritySlice(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectScalarVIRec](t, "reflect_scalar_vi_pos.db")

	if err := writeSets(c,
		[]uint64{1, 2, 3},
		[]*reflectScalarVIRec{
			{Code: reflectFoldedString("MiXeD")},
			{Code: reflectFoldedString("other")},
			{Code: reflectFoldedString("mixed")},
		},
	); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	got, err := readQueryKeys(c, qx.Query().SortBy(qx.POS("code", []reflectFoldedString{
		reflectFoldedString("other"),
		reflectFoldedString("mixed"),
	}), qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(SortBy POS code): %v", err)
	}
	assertUint64Slice(t, got, []uint64{2, 1, 3})
}

func TestReflectExt_QueryValueIndexerScalarUnderlyingSlice_RemainsScalar(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectBytesVIRec](t, "reflect_bytes_vi.db")

	if err := writeSet(c, 1, &reflectBytesVIRec{Key: reflectHexBytes{0xab, 0xcd}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	q := qx.Query(qx.EQ("key", reflectHexBytes{0xab, 0xcd}))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	cnt, err := readCount(c, q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("Count mismatch: got=%d want=1", cnt)
	}
}

func TestReflectExt_QueryValueIndexerPointerUnderlyingSlice_PreservesPointerLiteral(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectPtrBytesVIRec](t, "reflect_ptr_bytes_vi.db")

	key := reflectPtrHexBytes{0xab, 0xcd}
	if err := writeSet(c, 1, &reflectPtrBytesVIRec{Key: &key}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	queryKey := reflectPtrHexBytes{0xab, 0xcd}
	got, err := readQueryKeys(c, qx.Query(qx.EQ("key", &queryKey)))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})
}

func TestReflectExt_QueryValueIndexerScalarUnderlyingSlice_AllowsCanonicalString(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectBytesVIRec](t, "reflect_bytes_vi_plain.db")

	if err := writeSet(c, 1, &reflectBytesVIRec{Key: reflectHexBytes{0xab, 0xcd}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	q := qx.Query(qx.EQ("key", "abcd"))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})
}

func TestReflectExt_QueryValueIndexerScalarUnderlyingSlice_POSSort_RemainsScalar(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectBytesVIRec](t, "reflect_bytes_vi_pos.db")

	if err := writeSets(c,
		[]uint64{1, 2},
		[]*reflectBytesVIRec{
			{Key: reflectHexBytes{0xab, 0xcd}},
			{Key: reflectHexBytes{0xde, 0xf0}},
		},
	); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	got, err := readQueryKeys(c, qx.Query().SortBy(qx.POS("key", reflectHexBytes{0xde, 0xf0}), qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(SortBy POS key): %v", err)
	}
	assertUint64Slice(t, got, []uint64{2, 1})
}

func TestReflectExt_ValueIndexerDirectIfaceMap_QueryUnique(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectMapVIRec](t, "reflect_map_vi.db")

	if err := writeSet(c, 1, &reflectMapVIRec{Key: reflectMapVI{"id": "MiXeD"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	got, err := readQueryKeys(c, qx.Query(qx.EQ("key", "mixed")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	err = writeSet(c, 2, &reflectMapVIRec{Key: reflectMapVI{"id": "mixed"}})
	if err == nil {
		t.Fatalf("expected unique violation for duplicate map-backed ValueIndexer")
	}
	if !errors.Is(err, rbierrors.ErrUniqueViolation) && !strings.Contains(err.Error(), rbierrors.ErrUniqueViolation.Error()) {
		t.Fatalf("expected rbierrors.ErrUniqueViolation-compatible error, got: %v", err)
	}
}

func TestReflectExt_ValueIndexerDirectIfaceWordStruct_QueryUnique(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectPtrWordVIRec](t, "reflect_word_vi.db")

	label := "MiXeD"
	if err := writeSet(c, 1, &reflectPtrWordVIRec{Key: reflectPtrWordVI{Ptr: &label}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	got, err := readQueryKeys(c, qx.Query(qx.EQ("key", "mixed")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	dup := "mixed"
	err = writeSet(c, 2, &reflectPtrWordVIRec{Key: reflectPtrWordVI{Ptr: &dup}})
	if err == nil {
		t.Fatalf("expected unique violation for duplicate direct-iface struct ValueIndexer")
	}
	if !errors.Is(err, rbierrors.ErrUniqueViolation) && !strings.Contains(err.Error(), rbierrors.ErrUniqueViolation.Error()) {
		t.Fatalf("expected rbierrors.ErrUniqueViolation-compatible error, got: %v", err)
	}
}

func TestReflectExt_ValueIndexerInterfaceField_Unsupported(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	c, err := Open[uint64, reflectInterfaceVIRec](raw, testOptions(Options{}))
	if err == nil {
		_ = c.Close()
		t.Fatal("New accepted ValueIndexer interface field")
	}
}

func TestReflectExt_ValueIndexerInterfaceSlice_Unsupported(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	c, err := Open[uint64, reflectInterfaceVISliceRec](raw, testOptions(Options{}))
	if err == nil {
		_ = c.Close()
		t.Fatal("New accepted ValueIndexer interface slice field")
	}
}

func TestReflectExt_QueryNativeTimeScalar_UsesUnixSeconds(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectTimeRec](t, "reflect_time.db")

	base := time.Unix(1_700_000_000, 100_000_000).UTC()
	sameSec := time.Unix(base.Unix(), 900_000_000).UTC()
	later := base.Add(2 * time.Second)

	if err := writeSets(c,
		[]uint64{1, 2, 3},
		[]*reflectTimeRec{
			{When: base},
			{When: sameSec},
			{When: later},
		},
	); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	got, err := readQueryKeys(c, qx.Query(qx.EQ("when", base)))
	if err != nil {
		t.Fatalf("QueryKeys(EQ time.Time): %v", err)
	}
	assertUint64Set(t, got, []uint64{1, 2})

	got, err = readQueryKeys(c, qx.Query(qx.EQ("when", sameSec)))
	if err != nil {
		t.Fatalf("QueryKeys(EQ same-second time): %v", err)
	}
	assertUint64Set(t, got, []uint64{1, 2})

	got, err = readQueryKeys(c, qx.Query(qx.GTE("when", base.Add(time.Second))))
	if err != nil {
		t.Fatalf("QueryKeys(GTE time): %v", err)
	}
	assertUint64Slice(t, got, []uint64{3})

	got, err = readQueryKeys(c, qx.Query().Sort("when", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(Sort when ASC): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1, 2, 3})
}

func TestReflectExt_QueryNativeTimePointer_UsesUnixSecondsAndNilIndex(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectTimePtrRec](t, "reflect_time_ptr.db")

	early := time.Unix(1_700_000_100, 200_000_000).UTC()
	late := early.Add(3 * time.Second)

	if err := writeSets(c,
		[]uint64{1, 2, 3},
		[]*reflectTimePtrRec{
			{When: nil},
			{When: &early},
			{When: &late},
		},
	); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	got, err := readQueryKeys(c, qx.Query(qx.EQ("when", nil)))
	if err != nil {
		t.Fatalf("QueryKeys(EQ nil): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = readQueryKeys(c, qx.Query(qx.LT("when", late)))
	if err != nil {
		t.Fatalf("QueryKeys(LT time): %v", err)
	}
	assertUint64Slice(t, got, []uint64{2})

	got, err = readQueryKeys(c, qx.Query(qx.GTE("when", early)).Sort("when", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(GTE+Sort when ASC): %v", err)
	}
	assertUint64Slice(t, got, []uint64{2, 3})
}

func TestReflectExt_QueryNativeTimeScalar_POSSort_NormalizesPriorities(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectTimeRec](t, "reflect_time_pos.db")

	base := time.Unix(1_700_000_000, 100_000_000).UTC()
	sameSec := time.Unix(base.Unix(), 900_000_000).UTC()
	later := base.Add(2 * time.Second)

	if err := writeSets(c,
		[]uint64{1, 2, 3},
		[]*reflectTimeRec{
			{When: base},
			{When: sameSec},
			{When: later},
		},
	); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	got, err := readQueryKeys(c, qx.Query().SortBy(qx.POS("when", []time.Time{later, base}), qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(SortBy POS when): %v", err)
	}
	assertUint64Slice(t, got, []uint64{3, 1, 2})
}

func TestReflectExt_NamedNativeTimeIndexedTypes_Unsupported(t *testing.T) {
	assertNewRejectsNamedTime[reflectNamedTimeRec](t)
	assertNewRejectsNamedTime[reflectNamedTimePointerRec](t)
	assertNewRejectsNamedTime[reflectNamedTimePtrRec](t)
	assertNewRejectsNamedTime[reflectTimeVIRec](t)
	assertNewRejectsNamedTime[reflectPtrTimeVIRec](t)
}

func TestReflectExt_QueryMixedNumericAndTimeBounds_DoesNotAliasCache(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectInt64AgeRec](t, "reflect_int64_time_cache.db")

	if err := writeSet(c, 1, &reflectInt64AgeRec{Age: 15}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.GTE("age", time.Unix(10, 0)),
			qx.GTE("age", int64(10)),
		),
	)

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys(OR mixed time/int64): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})
}

func TestReflectExt_EmbeddedUnsafeAccessors_QueryUnique(t *testing.T) {
	c := openTempUint64CollectionReflect[reflectUnsafeAccessorRec](t, "reflect_embedded_accessors.db")

	code1 := reflectPtrFoldedString("MiXeD")
	count1 := uint64(5)
	if err := writeSet(c, 1, &reflectUnsafeAccessorRec{
		Name: "alice",
		ReflectUnsafeEmbeddedIndexed: ReflectUnsafeEmbeddedIndexed{
			Code:  &code1,
			Score: 10,
			Tags:  []string{"x", "x", "y"},
			Count: &count1,
		},
	}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	code2 := reflectPtrFoldedString("SeCoNd")
	count2 := uint64(8)
	if err := writeSet(c, 2, &reflectUnsafeAccessorRec{
		Name: "bob",
		ReflectUnsafeEmbeddedIndexed: ReflectUnsafeEmbeddedIndexed{
			Code:  &code2,
			Score: 20,
			Tags:  []string{"z"},
			Count: &count2,
		},
	}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	got, err := readQueryKeys(c, qx.Query(qx.EQ("code", "mixed")))
	if err != nil {
		t.Fatalf("QueryKeys(code): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = readQueryKeys(c, qx.Query(qx.EQ("score", 10)))
	if err != nil {
		t.Fatalf("QueryKeys(score): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = readQueryKeys(c, qx.Query(qx.HASANY("tags", []string{"y"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = readQueryKeys(c, qx.Query(qx.EQ("count", uint64(5))))
	if err != nil {
		t.Fatalf("QueryKeys(count): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	updatedCode := reflectPtrFoldedString("MIXED")
	updatedCount := uint64(11)
	if err := writeSets(c, []uint64{1}, []*reflectUnsafeAccessorRec{{
		Name: "alice",
		ReflectUnsafeEmbeddedIndexed: ReflectUnsafeEmbeddedIndexed{
			Code:  &updatedCode,
			Score: 11,
			Tags:  []string{"y", "w"},
			Count: &updatedCount,
		},
	}}); err != nil {
		t.Fatalf("MultiSet(update): %v", err)
	}

	got, err = readQueryKeys(c, qx.Query(qx.EQ("score", 11)))
	if err != nil {
		t.Fatalf("QueryKeys(updated score): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = readQueryKeys(c, qx.Query(qx.HASANY("tags", []string{"w"})))
	if err != nil {
		t.Fatalf("QueryKeys(updated tags): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = readQueryKeys(c, qx.Query(qx.EQ("count", uint64(11))))
	if err != nil {
		t.Fatalf("QueryKeys(updated count): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	dupCode := reflectPtrFoldedString("mixed")
	err = writeSet(c, 3, &reflectUnsafeAccessorRec{
		Name: "carol",
		ReflectUnsafeEmbeddedIndexed: ReflectUnsafeEmbeddedIndexed{
			Code:  &dupCode,
			Score: 30,
		},
	})
	if err == nil {
		t.Fatalf("expected unique violation for duplicate embedded ValueIndexer field")
	}
	if !errors.Is(err, rbierrors.ErrUniqueViolation) && !strings.Contains(err.Error(), rbierrors.ErrUniqueViolation.Error()) {
		t.Fatalf("expected rbierrors.ErrUniqueViolation-compatible error, got: %v", err)
	}
}
