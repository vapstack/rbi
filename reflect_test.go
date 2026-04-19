package rbi

import (
	"encoding/hex"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

type reflectFoldedString string

func (s reflectFoldedString) IndexingValue() string {
	return strings.ToLower(string(s))
}

type reflectScalarVIRec struct {
	Code reflectFoldedString `db:"code" dbi:"default"`
}

type reflectScalarVIUniqueRec struct {
	Code reflectFoldedString `db:"code" rbi:"unique"`
}

type reflectSliceVIRec struct {
	Tags []reflectFoldedString `db:"tags" dbi:"default"`
}

type reflectHexBytes []byte

func (b reflectHexBytes) IndexingValue() string {
	return hex.EncodeToString([]byte(b))
}

type reflectBytesVIRec struct {
	Key reflectHexBytes `db:"key" dbi:"default"`
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
	Tags []ValueIndexer `db:"tags" dbi:"default"`
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
	Score int                     `db:"score" dbi:"default"`
	Tags  []string                `db:"tags" dbi:"default"`
	Count *uint64                 `db:"count" dbi:"default"`
}

type reflectUnsafeAccessorRec struct {
	Name string `db:"name" dbi:"default"`
	ReflectUnsafeEmbeddedIndexed
}

type reflectPatchTimeRec struct {
	Name    string               `db:"name" dbi:"default"`
	When    time.Time            `db:"-"`
	Slots   []time.Time          `db:"-"`
	Windows map[time.Time]string `db:"-"`
}

type reflectNamedTag string
type reflectNamedTags []reflectNamedTag

type reflectNamedSlicePatchRec struct {
	Name string           `db:"name" dbi:"default"`
	Tags reflectNamedTags `db:"tags" dbi:"default"`
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
	Name      string              `db:"name" dbi:"default"`
	Nested    reflectPatchNested  `db:"-"`
	NestedPtr *reflectPatchNested `db:"-"`
}

type reflectPatchNestedSliceRec struct {
	Name  string                  `db:"name" dbi:"default"`
	Items []reflectPatchNestedRec `db:"-"`
}

type reflectNumericPatchRec struct {
	Name  string  `db:"name" dbi:"default"`
	I8    int8    `db:"-"`
	U8    uint8   `db:"-"`
	I64   int64   `db:"-"`
	U64   uint64  `db:"-"`
	Bytes []uint8 `db:"-"`
}

func openTempDBUint64Reflect[V any](t *testing.T, filename string, options ...Options) *DB[uint64, V] {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, filename)
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	opts.EnableAutoBatchStats = true
	opts.EnableSnapshotStats = true

	db, err := New[uint64, V](raw, opts)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db
}

func patchFieldsByName(fields []Field) map[string]any {
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		out[f.Name] = f.Value
	}
	return out
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
	db := openTempDBUint64Reflect[reflectScalarVIRec](t, "reflect_scalar_vi.db")

	if err := db.BatchSet(
		[]uint64{1, 2},
		[]*reflectScalarVIRec{
			{Code: reflectFoldedString("MiXeD")},
			{Code: reflectFoldedString("mixed")},
		},
	); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}

	q := qx.Query(qx.EQ("code", reflectFoldedString("MiXeD")))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Set(t, got, []uint64{1, 2})

	cnt, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("Count mismatch: got=%d want=2", cnt)
	}
}

func TestReflectExt_QueryValueIndexerScalarPlainString_UsesCanonicalKey(t *testing.T) {
	db := openTempDBUint64Reflect[reflectScalarVIRec](t, "reflect_scalar_vi_plain.db")

	if err := db.BatchSet(
		[]uint64{1, 2},
		[]*reflectScalarVIRec{
			{Code: reflectFoldedString("MiXeD")},
			{Code: reflectFoldedString("mixed")},
		},
	); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}

	q := qx.Query(qx.EQ("code", "mixed"))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Set(t, got, []uint64{1, 2})
}

func TestReflectExt_UniqueValueIndexerScalarNamedString_UsesIndexingValue(t *testing.T) {
	db := openTempDBUint64Reflect[reflectScalarVIUniqueRec](t, "reflect_scalar_vi_unique.db")

	if err := db.Set(1, &reflectScalarVIUniqueRec{Code: reflectFoldedString("MiXeD")}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	err := db.Set(2, &reflectScalarVIUniqueRec{Code: reflectFoldedString("mixed")})
	if err == nil {
		t.Fatalf("expected unique violation for equal IndexingValue()")
	}
	if !errors.Is(err, ErrUniqueViolation) && !strings.Contains(err.Error(), ErrUniqueViolation.Error()) {
		t.Fatalf("expected ErrUniqueViolation-compatible error, got: %v", err)
	}
}

func TestReflectExt_QueryValueIndexerSliceNamedString_NormalizesElements(t *testing.T) {
	db := openTempDBUint64Reflect[reflectSliceVIRec](t, "reflect_slice_vi.db")

	if err := db.BatchSet(
		[]uint64{1, 2},
		[]*reflectSliceVIRec{
			{Tags: []reflectFoldedString{reflectFoldedString("MiXeD")}},
			{Tags: []reflectFoldedString{reflectFoldedString("mixed")}},
		},
	); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}

	q := qx.Query(qx.HASANY("tags", []reflectFoldedString{reflectFoldedString("mixed")}))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Set(t, got, []uint64{1, 2})

	cnt, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("Count mismatch: got=%d want=2", cnt)
	}
}

func TestReflectExt_QueryValueIndexerSlicePlainStrings_UseCanonicalKeys(t *testing.T) {
	db := openTempDBUint64Reflect[reflectSliceVIRec](t, "reflect_slice_vi_plain.db")

	if err := db.BatchSet(
		[]uint64{1, 2},
		[]*reflectSliceVIRec{
			{Tags: []reflectFoldedString{reflectFoldedString("MiXeD")}},
			{Tags: []reflectFoldedString{reflectFoldedString("mixed")}},
		},
	); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}

	q := qx.Query(qx.HASANY("tags", []string{"mixed"}))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Set(t, got, []uint64{1, 2})
}

func TestReflectExt_QueryValueIndexerScalarUnderlyingSlice_RemainsScalar(t *testing.T) {
	db := openTempDBUint64Reflect[reflectBytesVIRec](t, "reflect_bytes_vi.db")

	if err := db.Set(1, &reflectBytesVIRec{Key: reflectHexBytes{0xab, 0xcd}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	q := qx.Query(qx.EQ("key", reflectHexBytes{0xab, 0xcd}))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	cnt, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("Count mismatch: got=%d want=1", cnt)
	}
}

func TestReflectExt_QueryValueIndexerScalarUnderlyingSlice_AllowsCanonicalString(t *testing.T) {
	db := openTempDBUint64Reflect[reflectBytesVIRec](t, "reflect_bytes_vi_plain.db")

	if err := db.Set(1, &reflectBytesVIRec{Key: reflectHexBytes{0xab, 0xcd}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	q := qx.Query(qx.EQ("key", "abcd"))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})
}

func TestReflectExt_ValueIndexerDirectIfaceMap_QueryUniqueAndRebuild(t *testing.T) {
	db := openTempDBUint64Reflect[reflectMapVIRec](t, "reflect_map_vi.db")

	if err := db.Set(1, &reflectMapVIRec{Key: reflectMapVI{"id": "MiXeD"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	got, err := db.QueryKeys(qx.Query(qx.EQ("key", "mixed")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	err = db.Set(2, &reflectMapVIRec{Key: reflectMapVI{"id": "mixed"}})
	if err == nil {
		t.Fatalf("expected unique violation for duplicate map-backed ValueIndexer")
	}
	if !errors.Is(err, ErrUniqueViolation) && !strings.Contains(err.Error(), ErrUniqueViolation.Error()) {
		t.Fatalf("expected ErrUniqueViolation-compatible error, got: %v", err)
	}
}

func TestReflectExt_ValueIndexerDirectIfaceWordStruct_QueryUniqueAndRebuild(t *testing.T) {
	db := openTempDBUint64Reflect[reflectPtrWordVIRec](t, "reflect_word_vi.db")

	label := "MiXeD"
	if err := db.Set(1, &reflectPtrWordVIRec{Key: reflectPtrWordVI{Ptr: &label}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	got, err := db.QueryKeys(qx.Query(qx.EQ("key", "mixed")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	dup := "mixed"
	err = db.Set(2, &reflectPtrWordVIRec{Key: reflectPtrWordVI{Ptr: &dup}})
	if err == nil {
		t.Fatalf("expected unique violation for duplicate direct-iface struct ValueIndexer")
	}
	if !errors.Is(err, ErrUniqueViolation) && !strings.Contains(err.Error(), ErrUniqueViolation.Error()) {
		t.Fatalf("expected ErrUniqueViolation-compatible error, got: %v", err)
	}
}

func TestReflectExt_ValueIndexerInterfaceField_InitAndWritePath(t *testing.T) {
	db := openTempDBUint64Reflect[reflectInterfaceVIRec](t, "reflect_interface_vi.db")

	if err := db.Set(1, &reflectInterfaceVIRec{Key: reflectMapVI{"id": "MiXeD"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	got, err := db.QueryKeys(qx.Query(qx.EQ("key", "mixed")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})
}

func TestReflectExt_ValueIndexerInterfaceSlice_InitAndWritePath(t *testing.T) {
	db := openTempDBUint64Reflect[reflectInterfaceVISliceRec](t, "reflect_interface_vi_slice.db")

	label := "SeCoNd"
	if err := db.Set(1, &reflectInterfaceVISliceRec{
		Tags: []ValueIndexer{
			reflectMapVI{"id": "MiXeD"},
			reflectPtrWordVI{Ptr: &label},
		},
	}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	got, err := db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"mixed"})))
	if err != nil {
		t.Fatalf("QueryKeys(mixed): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"second"})))
	if err != nil {
		t.Fatalf("QueryKeys(second): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})
}

func TestReflectExt_ModifiedIndexedFields_EmbeddedUnsafeAccessorPaths(t *testing.T) {
	db := openTempDBUint64Reflect[reflectUnsafeAccessorRec](t, "reflect_embedded_modified.db")

	oldCode := reflectPtrFoldedString("MiXeD")
	sameCode := reflectPtrFoldedString("mixed")
	oldCount := uint64(7)
	sameCount := uint64(7)

	base := &reflectUnsafeAccessorRec{
		Name: "alice",
		ReflectUnsafeEmbeddedIndexed: ReflectUnsafeEmbeddedIndexed{
			Code:  &oldCode,
			Score: 3,
			Tags:  []string{"x", "y"},
			Count: &oldCount,
		},
	}
	same := &reflectUnsafeAccessorRec{
		Name: "alice",
		ReflectUnsafeEmbeddedIndexed: ReflectUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 3,
			Tags:  []string{"x", "y"},
			Count: &sameCount,
		},
	}

	if mods := db.getModifiedIndexedFields(base, same); len(mods) != 0 {
		t.Fatalf("expected no modified indexed fields, got %v", mods)
	}
	var uniqueMods []string
	db.forEachModifiedAccessor(db.uniqueFieldAccessors, base, same, func(acc indexedFieldAccessor) bool {
		uniqueMods = append(uniqueMods, acc.name)
		return true
	})
	if len(uniqueMods) != 0 {
		t.Fatalf("expected no modified unique fields, got %v", uniqueMods)
	}

	nextCount := uint64(9)
	countChanged := &reflectUnsafeAccessorRec{
		Name: "alice",
		ReflectUnsafeEmbeddedIndexed: ReflectUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 3,
			Tags:  []string{"x", "y"},
			Count: &nextCount,
		},
	}
	if mods := db.getModifiedIndexedFields(base, countChanged); !slices.Equal(mods, []string{"count"}) {
		t.Fatalf("expected only count to change, got %v", mods)
	}

	scoreChanged := &reflectUnsafeAccessorRec{
		Name: "alice",
		ReflectUnsafeEmbeddedIndexed: ReflectUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 4,
			Tags:  []string{"x", "y"},
			Count: &sameCount,
		},
	}
	if mods := db.getModifiedIndexedFields(base, scoreChanged); !slices.Equal(mods, []string{"score"}) {
		t.Fatalf("expected only score to change, got %v", mods)
	}

	tagsChanged := &reflectUnsafeAccessorRec{
		Name: "alice",
		ReflectUnsafeEmbeddedIndexed: ReflectUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 3,
			Tags:  []string{"x", "z"},
			Count: &sameCount,
		},
	}
	if mods := db.getModifiedIndexedFields(base, tagsChanged); !slices.Equal(mods, []string{"tags"}) {
		t.Fatalf("expected only tags to change, got %v", mods)
	}
}

func TestReflectExt_EmbeddedUnsafeAccessors_QueryUniqueAndRebuild(t *testing.T) {
	db := openTempDBUint64Reflect[reflectUnsafeAccessorRec](t, "reflect_embedded_accessors.db")

	code1 := reflectPtrFoldedString("MiXeD")
	count1 := uint64(5)
	if err := db.Set(1, &reflectUnsafeAccessorRec{
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
	if err := db.Set(2, &reflectUnsafeAccessorRec{
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

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	got, err := db.QueryKeys(qx.Query(qx.EQ("code", "mixed")))
	if err != nil {
		t.Fatalf("QueryKeys(code): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = db.QueryKeys(qx.Query(qx.EQ("score", 10)))
	if err != nil {
		t.Fatalf("QueryKeys(score): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"y"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = db.QueryKeys(qx.Query(qx.EQ("count", uint64(5))))
	if err != nil {
		t.Fatalf("QueryKeys(count): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	updatedCode := reflectPtrFoldedString("MIXED")
	updatedCount := uint64(11)
	if err := db.BatchSet([]uint64{1}, []*reflectUnsafeAccessorRec{{
		Name: "alice",
		ReflectUnsafeEmbeddedIndexed: ReflectUnsafeEmbeddedIndexed{
			Code:  &updatedCode,
			Score: 11,
			Tags:  []string{"y", "w"},
			Count: &updatedCount,
		},
	}}); err != nil {
		t.Fatalf("BatchSet(update): %v", err)
	}

	got, err = db.QueryKeys(qx.Query(qx.EQ("score", 11)))
	if err != nil {
		t.Fatalf("QueryKeys(updated score): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"w"})))
	if err != nil {
		t.Fatalf("QueryKeys(updated tags): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	got, err = db.QueryKeys(qx.Query(qx.EQ("count", uint64(11))))
	if err != nil {
		t.Fatalf("QueryKeys(updated count): %v", err)
	}
	assertUint64Slice(t, got, []uint64{1})

	dupCode := reflectPtrFoldedString("mixed")
	err = db.Set(3, &reflectUnsafeAccessorRec{
		Name: "carol",
		ReflectUnsafeEmbeddedIndexed: ReflectUnsafeEmbeddedIndexed{
			Code:  &dupCode,
			Score: 30,
		},
	})
	if err == nil {
		t.Fatalf("expected unique violation for duplicate embedded ValueIndexer field")
	}
	if !errors.Is(err, ErrUniqueViolation) && !strings.Contains(err.Error(), ErrUniqueViolation.Error()) {
		t.Fatalf("expected ErrUniqueViolation-compatible error, got: %v", err)
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

	patch := db.MakePatch(oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotWhen, ok := fields["When"].(time.Time)
	if !ok {
		t.Fatalf("patch must contain time.Time value for When, got %#v", fields["When"])
	}
	if gotWhen != newVal.When {
		t.Fatalf("patch lost time value: got=%#v want=%#v", gotWhen, newVal.When)
	}

	applied := *oldVal
	if err := db.applyPatch(&applied, patch, false); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	if applied.When != newVal.When {
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

	patch := db.MakePatch(oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotSlots, ok := fields["Slots"].([]time.Time)
	if !ok {
		t.Fatalf("patch must contain []time.Time value for Slots, got %#v", fields["Slots"])
	}
	if !reflect.DeepEqual(gotSlots, newVal.Slots) {
		t.Fatalf("patch lost time slice contents: got=%#v want=%#v", gotSlots, newVal.Slots)
	}

	applied := *oldVal
	if err := db.applyPatch(&applied, patch, false); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	if !reflect.DeepEqual(applied.Slots, newVal.Slots) {
		t.Fatalf("patched record lost time slice contents: got=%#v want=%#v", applied.Slots, newVal.Slots)
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

	patch := db.MakePatch(oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotWindows, ok := fields["Windows"].(map[time.Time]string)
	if !ok {
		t.Fatalf("patch must contain map[time.Time]string value for Windows, got %#v", fields["Windows"])
	}
	if !reflect.DeepEqual(gotWindows, newVal.Windows) {
		t.Fatalf("patch lost time map contents: got=%#v want=%#v", gotWindows, newVal.Windows)
	}

	applied := *oldVal
	if err := db.applyPatch(&applied, patch, false); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	if !reflect.DeepEqual(applied.Windows, newVal.Windows) {
		t.Fatalf("patched record lost time map contents: got=%#v want=%#v", applied.Windows, newVal.Windows)
	}
}

func TestReflectExt_MakePatch_PreservesNamedSliceType(t *testing.T) {
	db := openTempDBUint64Reflect[reflectNamedSlicePatchRec](t, "reflect_patch_named_slice.db")

	oldVal := &reflectNamedSlicePatchRec{Name: "alice"}
	newVal := &reflectNamedSlicePatchRec{
		Name: "alice",
		Tags: reflectNamedTags{"go", "db"},
	}

	patch := db.MakePatch(oldVal, newVal)
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

	applied := *oldVal
	if err := db.applyPatch(&applied, patch, false); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	if !reflect.DeepEqual(applied.Tags, reflectNamedTags{"go", "db"}) {
		t.Fatalf("patched record lost named slice type/content: %#v", applied.Tags)
	}
}

func TestReflectExt_MakePatch_UsesFullFieldEqualityForValueIndexer(t *testing.T) {
	db := openTempDBUint64Reflect[reflectMapVIRec](t, "reflect_patch_value_indexer_full_equality.db")

	oldVal := &reflectMapVIRec{Key: reflectMapVI{"id": "A", "note": "old"}}
	newVal := &reflectMapVIRec{Key: reflectMapVI{"id": "a", "note": "new"}}

	patch := db.MakePatch(oldVal, newVal)
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

	applied := *oldVal
	if err := db.applyPatch(&applied, patch, false); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
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

	patch := db.MakePatch(oldVal, newVal)
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

	applied := *oldVal
	if err := db.applyPatch(&applied, patch, false); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
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

	patch := db.MakePatch(oldVal, newVal)
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

	applied := *oldVal
	if err := db.applyPatch(&applied, patch, false); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
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

	patch := db.MakePatch(oldVal, newVal)
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

	applied := *oldVal
	if err := db.applyPatch(&applied, patch, false); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
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
