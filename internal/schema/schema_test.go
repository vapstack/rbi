package schema

import (
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
)

type schemaTestVI string

func (v schemaTestVI) IndexingValue() string {
	return strings.ToLower(string(v))
}

type SchemaTestTagEmbedded struct {
	Email string `db:"email" rbi:"unique"`
}

type schemaTestTagRec struct {
	SchemaTestTagEmbedded
	Name     string `db:"name" rbi:"index"`
	Amount   int64  `db:"amount" rbi:"measure"`
	Disabled string `db:"disabled" rbi:"-"`
	Plain    string
}

type schemaTestDuplicateTagDBNameRec struct {
	Left  string `db:"dup" rbi:"index"`
	Right string `db:"dup" rbi:"index"`
}

type SchemaTestDuplicateTagLeft struct {
	ID string `rbi:"index"`
}

type SchemaTestDuplicateTagRight struct {
	ID string `rbi:"index"`
}

type schemaTestDuplicateTagPromotedRec struct {
	SchemaTestDuplicateTagLeft
	SchemaTestDuplicateTagRight
}

func TestCompileTagsCollectsIndexedUniqueMeasureAndPatch(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestTagRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(rt.Fields) != 2 {
		t.Fatalf("indexed fields=%d want 2", len(rt.Fields))
	}
	if _, ok := rt.Fields["name"]; !ok {
		t.Fatal("missing indexed db field name")
	}
	if f, ok := rt.Fields["email"]; !ok || !f.Unique {
		t.Fatalf("unique embedded field metadata=(%+v,%v)", f, ok)
	}
	if _, ok := rt.Fields["disabled"]; ok {
		t.Fatal("disabled field must not be indexed")
	}
	if _, ok := rt.MeasureFields["amount"]; !ok {
		t.Fatal("missing measure field")
	}
	if len(rt.Patch.Access) == 0 || rt.Patch.Fields["name"] == nil || rt.Patch.Fields["email"] == nil {
		t.Fatal("patch runtime did not include exported fields")
	}
	if !rt.HasUnique {
		t.Fatal("expected HasUnique")
	}
}

func TestCompileTagsRejectDuplicateIndexNames(t *testing.T) {
	tests := []struct {
		name string
		typ  reflect.Type
	}{
		{name: "db_tag", typ: reflect.TypeFor[schemaTestDuplicateTagDBNameRec]()},
		{name: "promoted_go_name", typ: reflect.TypeFor[schemaTestDuplicateTagPromotedRec]()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Compile(tc.typ, Config{}); err == nil || !strings.Contains(err.Error(), "ambiguous index field name") {
				t.Fatalf("Compile err=%v want ambiguous index field name rejection", err)
			}
		})
	}
}

type schemaTestValueReceiverVIPtrRec struct {
	Key *schemaTestVI `db:"key" rbi:"index"`
}

type schemaTestValueReceiverVIPtrSliceRec struct {
	Keys []*schemaTestVI `db:"keys" rbi:"index"`
}

type schemaTestPointerReceiverVIPtrRec struct {
	Key  *schemaTestPtrFoldedString   `db:"key" rbi:"index"`
	Keys []*schemaTestPtrFoldedString `db:"keys" rbi:"index"`
}

func TestCompileAllowsValueReceiverValueIndexerPointers(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestValueReceiverVIPtrRec](), Config{})
	if err != nil {
		t.Fatalf("scalar pointer Compile: %v", err)
	}
	if f := rt.Fields["key"]; f == nil || !f.UseVI || !f.Ptr || f.Slice || f.Kind != reflect.String {
		t.Fatalf("scalar pointer field=%+v", f)
	}

	rt, err = Compile(reflect.TypeFor[schemaTestValueReceiverVIPtrSliceRec](), Config{})
	if err != nil {
		t.Fatalf("pointer slice Compile: %v", err)
	}
	if f := rt.Fields["keys"]; f == nil || !f.UseVI || f.Ptr || !f.Slice {
		t.Fatalf("pointer slice field=%+v", f)
	}

	if _, err := Compile(reflect.TypeFor[schemaTestPointerReceiverVIPtrRec](), Config{}); err != nil {
		t.Fatalf("pointer receiver Compile: %v", err)
	}
}

type SchemaTestOptionLeft struct {
	ID string `db:"left_id"`
}

type SchemaTestOptionRight struct {
	ID string `db:"right_id"`
}

type schemaTestOptionRec struct {
	SchemaTestOptionLeft
	SchemaTestOptionRight
	Name   string `db:"name" rbi:"index"`
	Score  int64  `db:"score_db" rbi:"index"`
	Amount int64  `db:"amount"`
}

type SchemaTestOptionShadowedEmbedded struct {
	ID string `db:"embedded_id"`
}

type schemaTestOptionShadowedRec struct {
	SchemaTestOptionShadowedEmbedded
	ID string
}

type SchemaTestAnonymousID string

type schemaTestAnonymousTaggedIDRec struct {
	SchemaTestAnonymousID `rbi:"index"`
}

type schemaTestAnonymousOptionIDRec struct {
	SchemaTestAnonymousID
}

type schemaTestAnonymousTaggedTimeRec struct {
	time.Time `rbi:"index"`
}

type schemaTestNamedTime time.Time

type schemaTestNamedTimePtrRec struct {
	When *schemaTestNamedTime `db:"when" rbi:"index"`
}

type schemaTestUintptrSliceIndexRec struct {
	Values []uintptr `db:"values" rbi:"index"`
}

type SchemaTestAnonymousToken struct {
	Value string
}

func (v SchemaTestAnonymousToken) IndexingValue() string {
	return strings.ToLower(v.Value)
}

type schemaTestAnonymousTaggedVIRec struct {
	SchemaTestAnonymousToken `rbi:"index"`
}

type SchemaTestAnonymousVIEmbedded struct {
	Email string `db:"email" json:"email" rbi:"unique"`
}

func (v SchemaTestAnonymousVIEmbedded) IndexingValue() string {
	return strings.ToLower(v.Email)
}

type schemaTestAnonymousVIEmbeddedRec struct {
	SchemaTestAnonymousVIEmbedded
}

type SchemaTestSkippedAnonymousTagParent struct {
	SchemaTestAnonymousToken `rbi:"index"`
}

type schemaTestSkippedAnonymousTagRec struct {
	SchemaTestSkippedAnonymousTagParent `rbi:"-"`
}

type SchemaTestSkippedInvalidAnonymousTagParent struct {
	SchemaTestAnonymousContainer `rbi:"index,unique"`
}

type schemaTestSkippedInvalidAnonymousTagRec struct {
	SchemaTestSkippedInvalidAnonymousTagParent `rbi:"-"`
}

type SchemaTestAnonymousContainer struct {
	Value string
}

type schemaTestAnonymousTaggedContainerRec struct {
	SchemaTestAnonymousContainer `rbi:"index"`
}

type SchemaTestPointerEmbeddedTagged struct {
	Email string `rbi:"unique"`
}

type schemaTestPointerEmbeddedTaggedRec struct {
	*SchemaTestPointerEmbeddedTagged
}

type SchemaTestRecursivePointerEmbedded struct {
	*SchemaTestRecursivePointerEmbedded
}

type schemaTestRecursivePointerEmbeddedRec struct {
	*SchemaTestRecursivePointerEmbedded
}

type SchemaTestRecursivePointerEmbeddedTagged struct {
	*SchemaTestRecursivePointerEmbeddedTagged
	Email string `rbi:"index"`
}

type schemaTestRecursivePointerEmbeddedTaggedRec struct {
	*SchemaTestRecursivePointerEmbeddedTagged
}

type SchemaTestJSONHiddenAnonymous struct {
	Value string `json:"value"`
}

type schemaTestJSONHiddenAnonymousRec struct {
	SchemaTestJSONHiddenAnonymous `json:"-"`
}

type schemaTestMeasureOnlyRec struct {
	Amount int64 `db:"amount" rbi:"measure"`
}

type SchemaTestShadowedUniqueEmbedded struct {
	Code string `db:"inner_code" json:"innerCode" rbi:"unique"`
}

type schemaTestShadowedPatchRec struct {
	SchemaTestShadowedUniqueEmbedded
	Code string `db:"outer_code" json:"outerCode"`
}

type SchemaTestShadowedJSONOnlyIndexedEmbedded struct {
	ID string `json:"inner" rbi:"index"`
}

type schemaTestShadowedJSONOnlyIndexedRec struct {
	SchemaTestShadowedJSONOnlyIndexedEmbedded
	ID string
}

type SchemaTestShadowedJSONOnlyMeasureEmbedded struct {
	Score int64 `json:"innerScore" rbi:"measure"`
}

type schemaTestShadowedJSONOnlyMeasureRec struct {
	SchemaTestShadowedJSONOnlyMeasureEmbedded
	Score int64
}

type schemaTestReservedDBTagRec struct {
	Key string `db:"$key"`
}

type schemaTestReservedJSONTagRec struct {
	Key string `json:"$key,omitempty"`
}

type schemaTestReservedIndexedDBTagRec struct {
	Key string `db:"$key" rbi:"index"`
}

type schemaTestPatchDBAliasGoNameCollisionRec struct {
	Unique string `db:"Name" rbi:"unique"`
	Name   string
}

type schemaTestPatchJSONAliasGoNameCollisionRec struct {
	Unique string `json:"Name" rbi:"unique"`
	Name   string
}

type SchemaTestPatchShadowedDBTagEmbedded struct {
	ID string `db:"ID"`
}

type schemaTestPatchShadowedDBTagRec struct {
	SchemaTestPatchShadowedDBTagEmbedded
	ID string
}

type SchemaTestPatchShadowedJSONTagEmbedded struct {
	ID string `json:"ID"`
}

type schemaTestPatchShadowedJSONTagRec struct {
	SchemaTestPatchShadowedJSONTagEmbedded
	ID string
}

type SchemaTestPatchSameNameLeft struct {
	ID string
}

type SchemaTestPatchSameNameRight struct {
	ID string
}

type schemaTestPatchSameNamePromotedRec struct {
	SchemaTestPatchSameNameLeft
	SchemaTestPatchSameNameRight
}

func TestPatchNameTouchesUniqueWithShadowedEmbeddedAliases(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestShadowedPatchRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !rt.PatchNameTouchesUnique("inner_code") {
		t.Fatal("db alias for shadowed embedded unique field was not marked unique")
	}
	if !rt.PatchNameTouchesUnique("innerCode") {
		t.Fatal("json alias for shadowed embedded unique field was not marked unique")
	}
	innerAcc := rt.IndexedByName["inner_code"]
	if innerAcc.PatchOrdinal < 0 {
		t.Fatal("shadowed embedded unique field did not get a patch ordinal")
	}
	if rt.Patch.Access[innerAcc.PatchOrdinal].Field.DBName != "inner_code" {
		t.Fatalf("shadowed embedded unique PatchOrdinal=%d field=%+v", innerAcc.PatchOrdinal, rt.Patch.Access[innerAcc.PatchOrdinal].Field)
	}
	if rt.PatchNameTouchesUnique("Code") {
		t.Fatal("shadowing Go name was marked unique for the outer field")
	}
	if rt.PatchNameTouchesUnique("outer_code") {
		t.Fatal("outer non-indexed db alias was marked unique")
	}
	if rt.PatchNameTouchesUnique("outerCode") {
		t.Fatal("outer non-indexed json alias was marked unique")
	}
}

func TestPatchRuntimeKeepsQueryNameForShadowedJSONOnlyAliases(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestShadowedJSONOnlyIndexedRec](), Config{})
	if err != nil {
		t.Fatalf("Compile index: %v", err)
	}
	if got := rt.Patch.Fields["inner"].DBName; got != "" {
		t.Fatalf("shadowed indexed JSON alias DBName=%q want hidden default name", got)
	}
	if got := rt.Patch.Fields["inner"].QueryName; got != "ID" {
		t.Fatalf("shadowed indexed JSON alias QueryName=%q want %q", got, "ID")
	}
	if got := rt.Patch.Fields["ID"].DBName; got != "ID" {
		t.Fatalf("shadowing non-indexed field DBName=%q want safe patch name", got)
	}
	if got := rt.Patch.Fields["ID"].QueryName; got != "" {
		t.Fatalf("shadowing non-indexed field QueryName=%q want no query name", got)
	}

	rt, err = Compile(reflect.TypeFor[schemaTestShadowedJSONOnlyMeasureRec](), Config{})
	if err != nil {
		t.Fatalf("Compile measure: %v", err)
	}
	if got := rt.Patch.Fields["innerScore"].DBName; got != "" {
		t.Fatalf("shadowed measure JSON alias DBName=%q want hidden default name", got)
	}
	if got := rt.Patch.Fields["innerScore"].QueryName; got != "Score" {
		t.Fatalf("shadowed measure JSON alias QueryName=%q want %q", got, "Score")
	}
	if got := rt.Patch.Fields["Score"].DBName; got != "Score" {
		t.Fatalf("shadowing non-measure field DBName=%q want safe patch name", got)
	}
	if got := rt.Patch.Fields["Score"].QueryName; got != "" {
		t.Fatalf("shadowing non-measure field QueryName=%q want no query name", got)
	}
}

func TestCompileOmitsSameNamePromotedPatchFields(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestPatchSameNamePromotedRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, ok := rt.Patch.Fields["ID"]; ok {
		t.Fatal("ambiguous promoted Go name was registered as patch field")
	}
	if len(rt.Patch.Access) != 2 {
		t.Fatalf("ambiguous promoted fields were not retained for MakePatch access: %d", len(rt.Patch.Access))
	}
	for _, acc := range rt.Patch.Access {
		if acc.Field.DBName != "" || acc.Field.JSONName != "" {
			t.Fatalf("ambiguous promoted field was left with unsafe names: %+v", acc.Field)
		}
	}

	rec := schemaTestPatchSameNamePromotedRec{
		SchemaTestPatchSameNameLeft:  SchemaTestPatchSameNameLeft{ID: "left"},
		SchemaTestPatchSameNameRight: SchemaTestPatchSameNameRight{ID: "right"},
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "ID", Value: "mutated"}}, false); err == nil || !strings.Contains(err.Error(), "cannot patch field ID") {
		t.Fatalf("Apply ambiguous promoted Go name err=%v", err)
	}
	if rec.SchemaTestPatchSameNameLeft.ID != "left" || rec.SchemaTestPatchSameNameRight.ID != "right" {
		t.Fatalf("ambiguous promoted Go name changed record: %+v", rec)
	}
}

func TestCompileRejectsPatchAliasGoNameCollisions(t *testing.T) {
	tests := []struct {
		name string
		typ  reflect.Type
		want string
	}{
		{name: "db", typ: reflect.TypeFor[schemaTestPatchDBAliasGoNameCollisionRec](), want: "ambiguous patch field name"},
		{name: "json", typ: reflect.TypeFor[schemaTestPatchJSONAliasGoNameCollisionRec](), want: "ambiguous patch field name"},
		{name: "shadowed_db", typ: reflect.TypeFor[schemaTestPatchShadowedDBTagRec](), want: "ambiguous db tag"},
		{name: "shadowed_json", typ: reflect.TypeFor[schemaTestPatchShadowedJSONTagRec](), want: "ambiguous json tag"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Compile(tc.typ, Config{}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Compile err=%v want %q rejection", err, tc.want)
			}
		})
	}
}

func TestCompileRejectsReservedKeyNames(t *testing.T) {
	tests := []struct {
		name string
		typ  reflect.Type
		cfg  Config
	}{
		{
			name: "options_index",
			typ:  reflect.TypeFor[schemaTestOptionRec](),
			cfg: Config{Index: map[string]IndexKind{
				ReservedKeyFieldName: IndexDefault,
			}},
		},
		{
			name: "db_tag_patch_alias",
			typ:  reflect.TypeFor[schemaTestReservedDBTagRec](),
		},
		{
			name: "json_tag_patch_alias",
			typ:  reflect.TypeFor[schemaTestReservedJSONTagRec](),
		},
		{
			name: "indexed_db_tag",
			typ:  reflect.TypeFor[schemaTestReservedIndexedDBTagRec](),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Compile(tc.typ, tc.cfg); err == nil || !strings.Contains(err.Error(), ReservedKeyFieldName) {
				t.Fatalf("Compile err=%v want reserved %q rejection", err, ReservedKeyFieldName)
			}
		})
	}
}

func TestCompileOptionsNilEmptyAndNameResolution(t *testing.T) {
	vtype := reflect.TypeFor[schemaTestOptionRec]()
	rt, err := Compile(vtype, Config{})
	if err != nil {
		t.Fatalf("Compile tags: %v", err)
	}
	if _, ok := rt.Fields["name"]; !ok {
		t.Fatal("nil Options.Index must collect tags")
	}
	if _, ok := rt.Patch.Fields["ID"]; ok {
		t.Fatal("ambiguous embedded Go patch name was registered")
	}
	if rt.Patch.Fields["left_id"] == nil || rt.Patch.Fields["right_id"] == nil {
		t.Fatal("db aliases for ambiguous embedded Go patch name were not registered")
	}
	if rt.Patch.Fields["left_id"].JSONName != "" || rt.Patch.Fields["right_id"].JSONName != "" {
		t.Fatalf("PatchJSON names for ambiguous embedded Go name were marked safe: left=%q right=%q", rt.Patch.Fields["left_id"].JSONName, rt.Patch.Fields["right_id"].JSONName)
	}
	var patched schemaTestOptionRec
	if err = rt.Patch.Apply(unsafe.Pointer(&patched), []PatchItem{{Name: "left_id", Value: "left"}, {Name: "right_id", Value: "right"}}, false); err != nil {
		t.Fatalf("Apply db aliases for ambiguous embedded Go name: %v", err)
	}
	if patched.SchemaTestOptionLeft.ID != "left" || patched.SchemaTestOptionRight.ID != "right" {
		t.Fatalf("db aliases patched wrong embedded fields: %+v", patched)
	}

	rt, err = Compile(vtype, Config{Index: map[string]IndexKind{}})
	if err != nil {
		t.Fatalf("Compile empty options: %v", err)
	}
	if rt.HasQueryFields() {
		t.Fatal("empty Options.Index must disable indexed/measure fields")
	}

	rt, err = Compile(vtype, Config{Index: map[string]IndexKind{
		"Name":     IndexDefault,
		"score_db": IndexUnique,
		"Amount":   IndexMeasure,
		"left_id":  IndexDefault,
		"right_id": IndexDefault,
	}})
	if err != nil {
		t.Fatalf("Compile options: %v", err)
	}
	if _, ok := rt.Fields["name"]; !ok {
		t.Fatal("Go-name option did not resolve")
	}
	if f := rt.Fields["score_db"]; f == nil || !f.Unique {
		t.Fatalf("db-name unique option metadata=%+v", f)
	}
	if _, ok := rt.MeasureFields["amount"]; !ok {
		t.Fatal("measure option did not resolve")
	}
	if _, ok := rt.Fields["left_id"]; !ok {
		t.Fatal("left db tag option did not resolve")
	}
	if _, ok := rt.Fields["right_id"]; !ok {
		t.Fatal("right db tag option did not resolve")
	}

	rt, err = Compile(reflect.TypeFor[schemaTestOptionShadowedRec](), Config{Index: map[string]IndexKind{"ID": IndexDefault}})
	if err != nil {
		t.Fatalf("Compile shadowed Go option: %v", err)
	}
	if f := rt.Fields["ID"]; f == nil || !slices.Equal(f.Index, []int{1}) {
		t.Fatalf("shadowed Go option field=%+v want top-level ID", f)
	}
	if _, ok := rt.Fields["embedded_id"]; ok {
		t.Fatal("shadowed promoted field was indexed by Go name option")
	}

	if _, err = Compile(vtype, Config{Index: map[string]IndexKind{"ID": IndexDefault}}); err == nil || !strings.Contains(err.Error(), `ambiguous Go field name "ID"`) {
		t.Fatalf("ambiguous Go name err=%v", err)
	}
	if _, err = Compile(vtype, Config{Index: map[string]IndexKind{"Missing": IndexDefault}}); err == nil || !strings.Contains(err.Error(), `unknown index field "Missing"`) {
		t.Fatalf("unknown option err=%v", err)
	}
	if _, err = Compile(vtype, Config{Index: map[string]IndexKind{"Name": IndexKind(99)}}); err == nil || !strings.Contains(err.Error(), `invalid IndexKind 99`) {
		t.Fatalf("invalid kind err=%v", err)
	}
	if _, err = Compile(vtype, Config{Index: map[string]IndexKind{"Score": IndexDefault, "score_db": IndexDefault}}); err == nil || !strings.Contains(err.Error(), `indexed more than once`) {
		t.Fatalf("duplicate option err=%v", err)
	}
}

func TestCompileAnonymousTaggedIndexFields(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAnonymousTaggedIDRec](), Config{})
	if err != nil {
		t.Fatalf("Compile anonymous ID: %v", err)
	}
	if f := rt.Fields["SchemaTestAnonymousID"]; f == nil || !slices.Equal(f.Index, []int{0}) {
		t.Fatalf("anonymous ID index field=%+v", f)
	}
	if acc, ok := rt.IndexedByName["SchemaTestAnonymousID"]; !ok || acc.PatchOrdinal < 0 {
		t.Fatalf("anonymous ID index accessor=(%+v,%v)", acc, ok)
	}
	if f := rt.Patch.Fields["SchemaTestAnonymousID"]; f == nil || !slices.Equal(f.Index, []int{0}) {
		t.Fatalf("anonymous ID patch field=%+v", f)
	}

	rt, err = Compile(reflect.TypeFor[schemaTestAnonymousTaggedTimeRec](), Config{})
	if err != nil {
		t.Fatalf("Compile anonymous time: %v", err)
	}
	if f := rt.Fields["Time"]; f == nil || !IsNativeTimeField(f) || !slices.Equal(f.Index, []int{0}) {
		t.Fatalf("anonymous time index field=%+v", f)
	}
	if acc, ok := rt.IndexedByName["Time"]; !ok || acc.PatchOrdinal < 0 {
		t.Fatalf("anonymous time index accessor=(%+v,%v)", acc, ok)
	}

	rt, err = Compile(reflect.TypeFor[schemaTestNamedTimePtrRec](), Config{})
	if err != nil {
		t.Fatalf("Compile named time pointer: %v", err)
	}
	if f := rt.Fields["when"]; f == nil || !IsNativeTimeField(f) || !f.Ptr || !slices.Equal(f.Index, []int{0}) {
		t.Fatalf("named time pointer index field=%+v", f)
	}
	if acc, ok := rt.IndexedByName["when"]; !ok || acc.PatchOrdinal < 0 {
		t.Fatalf("named time pointer index accessor=(%+v,%v)", acc, ok)
	}

	rt, err = Compile(reflect.TypeFor[schemaTestAnonymousTaggedVIRec](), Config{})
	if err != nil {
		t.Fatalf("Compile anonymous ValueIndexer: %v", err)
	}
	if f := rt.Fields["SchemaTestAnonymousToken"]; f == nil || !f.UseVI || !slices.Equal(f.Index, []int{0}) {
		t.Fatalf("anonymous ValueIndexer index field=%+v", f)
	} else if f.VIType != fieldTypeID(reflect.TypeFor[SchemaTestAnonymousToken]()) {
		t.Fatalf("anonymous ValueIndexer VIType=%q", f.VIType)
	}
	if acc, ok := rt.IndexedByName["SchemaTestAnonymousToken"]; !ok || acc.PatchOrdinal < 0 {
		t.Fatalf("anonymous ValueIndexer index accessor=(%+v,%v)", acc, ok)
	}
	if f := rt.Patch.Fields["Value"]; f == nil || !slices.Equal(f.Index, []int{0, 0}) || f.QueryName != "SchemaTestAnonymousToken" {
		t.Fatalf("anonymous ValueIndexer child patch field=%+v", f)
	}

	if _, err = Compile(reflect.TypeFor[schemaTestAnonymousTaggedContainerRec](), Config{}); err == nil || !strings.Contains(err.Error(), "cannot index field SchemaTestAnonymousContainer") {
		t.Fatalf("anonymous non-indexable container err=%v", err)
	}
	if _, err = Compile(reflect.TypeFor[schemaTestUintptrSliceIndexRec](), Config{}); err == nil || !strings.Contains(err.Error(), "slice elements must either be of a simple type") {
		t.Fatalf("uintptr slice index err=%v", err)
	}
}

func TestCompileUntaggedAnonymousValueIndexerStructPromotesChildren(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAnonymousVIEmbeddedRec](), Config{})
	if err != nil {
		t.Fatalf("Compile tags: %v", err)
	}
	if f := rt.Fields["email"]; f == nil || !f.Unique || !slices.Equal(f.Index, []int{0, 0}) {
		t.Fatalf("embedded ValueIndexer child index field=%+v", f)
	}
	if f := rt.Patch.Fields["email"]; f == nil || !slices.Equal(f.Index, []int{0, 0}) {
		t.Fatalf("embedded ValueIndexer child patch field=%+v", f)
	}
	if _, ok := rt.Patch.Fields["SchemaTestAnonymousVIEmbedded"]; ok {
		t.Fatal("untagged embedded ValueIndexer parent was registered as patch field")
	}

	rt, err = Compile(reflect.TypeFor[schemaTestAnonymousVIEmbeddedRec](), Config{Index: map[string]IndexKind{
		"email": IndexDefault,
	}})
	if err != nil {
		t.Fatalf("Compile options: %v", err)
	}
	if f := rt.Fields["email"]; f == nil || !slices.Equal(f.Index, []int{0, 0}) {
		t.Fatalf("embedded ValueIndexer child option field=%+v", f)
	}
	if _, ok := rt.Patch.Fields["SchemaTestAnonymousVIEmbedded"]; ok {
		t.Fatal("options mode registered untagged embedded ValueIndexer parent as patch field")
	}

	rt, err = Compile(reflect.TypeFor[schemaTestAnonymousVIEmbeddedRec](), Config{Index: map[string]IndexKind{
		"SchemaTestAnonymousVIEmbedded": IndexDefault,
	}})
	if err != nil {
		t.Fatalf("Compile anonymous ValueIndexer option: %v", err)
	}
	if f := rt.Fields["SchemaTestAnonymousVIEmbedded"]; f == nil || !f.UseVI || !slices.Equal(f.Index, []int{0}) {
		t.Fatalf("anonymous ValueIndexer option field=%+v", f)
	} else if f.VIType != fieldTypeID(reflect.TypeFor[schemaTestAnonymousVIEmbeddedRec]().Field(0).Type) {
		t.Fatalf("anonymous ValueIndexer option VIType=%q", f.VIType)
	}
	if acc, ok := rt.IndexedByName["SchemaTestAnonymousVIEmbedded"]; !ok || acc.PatchOrdinal < 0 {
		t.Fatalf("anonymous ValueIndexer option accessor=(%+v,%v)", acc, ok)
	}
	if f := rt.Patch.Fields["SchemaTestAnonymousVIEmbedded"]; f == nil || !slices.Equal(f.Index, []int{0}) {
		t.Fatalf("anonymous ValueIndexer option patch field=%+v", f)
	}
	if f := rt.Patch.Fields["email"]; f == nil || !slices.Equal(f.Index, []int{0, 0}) || f.QueryName != "SchemaTestAnonymousVIEmbedded" {
		t.Fatalf("anonymous ValueIndexer option child patch field=%+v", f)
	}

	rt, err = Compile(reflect.TypeFor[schemaTestAnonymousVIEmbeddedRec](), Config{Index: map[string]IndexKind{
		"SchemaTestAnonymousVIEmbedded": IndexUnique,
	}})
	if err != nil {
		t.Fatalf("Compile anonymous ValueIndexer unique option: %v", err)
	}
	if !rt.PatchNameTouchesUnique("email") {
		t.Fatal("anonymous ValueIndexer child patch field was not marked as touching parent unique index")
	}

	rt, err = Compile(reflect.TypeFor[schemaTestAnonymousVIEmbeddedRec](), Config{Index: map[string]IndexKind{
		"SchemaTestAnonymousVIEmbedded": IndexDefault,
		"email":                         IndexUnique,
	}})
	if err != nil {
		t.Fatalf("Compile overlapping anonymous ValueIndexer options: %v", err)
	}
	parentField := rt.Patch.Fields["SchemaTestAnonymousVIEmbedded"]
	if parentField == nil || parentField.QueryName != "SchemaTestAnonymousVIEmbedded" || !slices.Contains(parentField.QueryNames, "email") || !parentField.Unique {
		t.Fatalf("overlapping anonymous ValueIndexer parent patch field=%+v", parentField)
	}
	childField := rt.Patch.Fields["email"]
	if childField == nil || childField.QueryName != "email" || !slices.Contains(childField.QueryNames, "SchemaTestAnonymousVIEmbedded") || !childField.Unique {
		t.Fatalf("overlapping anonymous ValueIndexer child patch field=%+v", childField)
	}
}

func TestCompileSkippedAnonymousSubtreeIgnoresNestedIndexTags(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestSkippedAnonymousTagRec](), Config{})
	if err != nil {
		t.Fatalf("Compile skipped subtree: %v", err)
	}
	if rt.HasQueryFields() {
		t.Fatal("skipped embedded subtree produced query fields")
	}
	if _, ok := rt.Patch.Fields["SchemaTestAnonymousToken"]; ok {
		t.Fatal("skipped subtree interpreted nested anonymous index tag as parent patch field")
	}
	if f := rt.Patch.Fields["Value"]; f == nil || !slices.Equal(f.Index, []int{0, 0, 0}) {
		t.Fatalf("skipped subtree promoted child patch field=%+v", f)
	}

	if _, err = Compile(reflect.TypeFor[schemaTestSkippedInvalidAnonymousTagRec](), Config{}); err != nil {
		t.Fatalf("Compile skipped subtree with invalid nested anonymous tag: %v", err)
	}
}

func TestCompileRejectsPointerEmbeddedSubtreeIndexTags(t *testing.T) {
	_, err := Compile(reflect.TypeFor[schemaTestPointerEmbeddedTaggedRec](), Config{})
	if err == nil || !strings.Contains(err.Error(), "anonymous embedded pointer field SchemaTestPointerEmbeddedTagged cannot promote rbi tags") {
		t.Fatalf("Compile pointer embedded tags err=%v", err)
	}

	rt, err := Compile(reflect.TypeFor[schemaTestRecursivePointerEmbeddedRec](), Config{})
	if err != nil {
		t.Fatalf("Compile recursive pointer embedded without tags: %v", err)
	}
	if rt.HasQueryFields() {
		t.Fatal("recursive pointer embedded without tags produced query fields")
	}

	_, err = Compile(reflect.TypeFor[schemaTestRecursivePointerEmbeddedTaggedRec](), Config{})
	if err == nil || !strings.Contains(err.Error(), "anonymous embedded pointer field SchemaTestRecursivePointerEmbeddedTagged cannot promote rbi tags") {
		t.Fatalf("Compile recursive pointer embedded tags err=%v", err)
	}
}

func TestCompileJSONHiddenAnonymousSubtreeOmitsPromotedJSONNames(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestJSONHiddenAnonymousRec](), Config{})
	if err != nil {
		t.Fatalf("Compile json hidden anonymous subtree: %v", err)
	}
	if f := rt.Patch.Fields["Value"]; f == nil || f.JSONName != "" {
		t.Fatalf("hidden anonymous child patch field=%+v", f)
	}
	if f := rt.Patch.Fields["value"]; f != nil {
		t.Fatalf("hidden anonymous child json alias was registered: %+v", f)
	}
}

func TestCompileOptionsAnonymousFieldsHavePatchAccess(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAnonymousOptionIDRec](), Config{Index: map[string]IndexKind{
		"SchemaTestAnonymousID": IndexDefault,
	}})
	if err != nil {
		t.Fatalf("Compile anonymous option ID: %v", err)
	}
	if f := rt.Fields["SchemaTestAnonymousID"]; f == nil || !slices.Equal(f.Index, []int{0}) {
		t.Fatalf("anonymous option ID index field=%+v", f)
	}
	if acc, ok := rt.IndexedByName["SchemaTestAnonymousID"]; !ok || acc.PatchOrdinal < 0 {
		t.Fatalf("anonymous option ID index accessor=(%+v,%v)", acc, ok)
	}
	if f := rt.Patch.Fields["SchemaTestAnonymousID"]; f == nil || !slices.Equal(f.Index, []int{0}) {
		t.Fatalf("anonymous option ID patch field=%+v", f)
	}

	var rec schemaTestAnonymousOptionIDRec
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "SchemaTestAnonymousID", Value: "patched"}}, false); err != nil {
		t.Fatalf("Apply anonymous option ID: %v", err)
	}
	if rec.SchemaTestAnonymousID != "patched" {
		t.Fatalf("anonymous option ID patch value=%q", rec.SchemaTestAnonymousID)
	}
}

func TestCompileMeasureOnlyHasNoOrdinaryIndexedFields(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestMeasureOnlyRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(rt.Indexed) != 0 {
		t.Fatalf("indexed fields=%d want 0", len(rt.Indexed))
	}
	if len(rt.Fields) != 0 {
		t.Fatalf("ordinary fields=%d want 0", len(rt.Fields))
	}
	if _, ok := rt.MeasureFields["amount"]; !ok {
		t.Fatal("missing measure field")
	}
}

type schemaTestAccessorRec struct {
	Name   string       `db:"name" rbi:"index"`
	Age    int64        `db:"age" rbi:"index"`
	Score  float64      `db:"score" rbi:"index"`
	When   time.Time    `db:"when" rbi:"index"`
	Maybe  *int64       `db:"maybe" rbi:"index"`
	Tags   []string     `db:"tags" rbi:"index"`
	Scores []int64      `db:"scores" rbi:"index"`
	Code   schemaTestVI `db:"code" rbi:"index"`
	Unique string       `db:"unique" rbi:"unique"`
}

type schemaTestStableOrdinalRec struct {
	Z string `rbi:"index"`
	A string `rbi:"index"`
	M int    `rbi:"index"`
}

type schemaTestPtrFoldedString string

func (s *schemaTestPtrFoldedString) IndexingValue() string {
	if s == nil {
		return "<nil>"
	}
	return strings.ToLower(string(*s))
}

type SchemaTestUnsafeEmbeddedIndexed struct {
	Code  *schemaTestPtrFoldedString `db:"code" rbi:"unique"`
	Score int                        `db:"score" rbi:"index"`
	Tags  []string                   `db:"tags" rbi:"index"`
	Count *uint64                    `db:"count" rbi:"index"`
}

type schemaTestUnsafeAccessorRec struct {
	Name string `db:"name" rbi:"index"`
	SchemaTestUnsafeEmbeddedIndexed
}

func TestIndexedAccessorsAssignStableSortedOrdinals(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestStableOrdinalRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	got := make([]string, len(rt.Indexed))
	for i, acc := range rt.Indexed {
		got[i] = acc.Name
		if acc.Ordinal != i {
			t.Fatalf("ordinal for %q=%d want %d", acc.Name, acc.Ordinal, i)
		}
	}
	if !reflect.DeepEqual(got, []string{"A", "M", "Z"}) {
		t.Fatalf("ordinals=%v", got)
	}
}

func TestIndexedAccessorsEmitKeysAndDetectChanges(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	maybe := int64(-3)
	oldRec := schemaTestAccessorRec{
		Name:   "alice",
		Age:    -7,
		Score:  1.5,
		When:   time.Unix(10, 99).UTC(),
		Maybe:  &maybe,
		Tags:   []string{"go", "db", "go"},
		Scores: []int64{5, 7, 5},
		Code:   "ABC",
		Unique: "u1",
	}
	newRec := oldRec
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)

	var scratch WriteScratch
	rt.IndexedByName["name"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.strings, []string{"alice"}) {
		t.Fatalf("name scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["age"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedInt64Key(-7)}) {
		t.Fatalf("age scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["score"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedFloat64Key(1.5)}) {
		t.Fatalf("score scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["when"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedInt64Key(10)}) {
		t.Fatalf("time scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["maybe"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedInt64Key(-3)}) {
		t.Fatalf("ptr scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["tags"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || scratch.length != 2 || !slices.Equal(scratch.strings, []string{"go", "db"}) {
		t.Fatalf("tags scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["scores"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || scratch.length != 2 || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedInt64Key(5), keycodec.OrderedInt64Key(7)}) {
		t.Fatalf("scores scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["code"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.strings, []string{"abc"}) {
		t.Fatalf("ValueIndexer scratch=%+v", scratch)
	}

	if rt.IndexedByName["tags"].Modified(oldPtr, newPtr) {
		t.Fatal("identical slice values must not be modified")
	}
	newRec.Tags = []string{"go", "search"}
	if !rt.IndexedByName["tags"].Modified(oldPtr, unsafe.Pointer(&newRec)) {
		t.Fatal("changed slice value was not detected")
	}
	key, ok, isNil := rt.IndexedByName["unique"].UniqueGetter(oldPtr)
	if !ok || isNil || key.StringKey() != "u1" {
		t.Fatalf("unique getter key=%+v ok=%v isNil=%v", key, ok, isNil)
	}
}

func TestIndexedAccessorsDetectEmbeddedUnsafePathChanges(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestUnsafeAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	oldCode := schemaTestPtrFoldedString("MiXeD")
	sameCode := schemaTestPtrFoldedString("mixed")
	oldCount := uint64(7)
	sameCount := uint64(7)

	base := schemaTestUnsafeAccessorRec{
		Name: "alice",
		SchemaTestUnsafeEmbeddedIndexed: SchemaTestUnsafeEmbeddedIndexed{
			Code:  &oldCode,
			Score: 3,
			Tags:  []string{"x", "y"},
			Count: &oldCount,
		},
	}
	same := schemaTestUnsafeAccessorRec{
		Name: "alice",
		SchemaTestUnsafeEmbeddedIndexed: SchemaTestUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 3,
			Tags:  []string{"x", "y"},
			Count: &sameCount,
		},
	}

	basePtr := unsafe.Pointer(&base)
	samePtr := unsafe.Pointer(&same)

	var mods []string
	for i := range rt.Indexed {
		if rt.Indexed[i].Modified(basePtr, samePtr) {
			mods = append(mods, rt.Indexed[i].Name)
		}
	}
	if len(mods) != 0 {
		t.Fatalf("expected no modified indexed fields, got %v", mods)
	}
	var uniqueMods []string
	for i := range rt.Unique {
		if rt.Unique[i].Modified(basePtr, samePtr) {
			uniqueMods = append(uniqueMods, rt.Unique[i].Name)
		}
	}
	if len(uniqueMods) != 0 {
		t.Fatalf("expected no modified unique fields, got %v", uniqueMods)
	}

	nextCount := uint64(9)
	countChanged := schemaTestUnsafeAccessorRec{
		Name: "alice",
		SchemaTestUnsafeEmbeddedIndexed: SchemaTestUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 3,
			Tags:  []string{"x", "y"},
			Count: &nextCount,
		},
	}
	mods = mods[:0]
	nextPtr := unsafe.Pointer(&countChanged)
	for i := range rt.Indexed {
		if rt.Indexed[i].Modified(basePtr, nextPtr) {
			mods = append(mods, rt.Indexed[i].Name)
		}
	}
	if !slices.Equal(mods, []string{"count"}) {
		t.Fatalf("expected only count to change, got %v", mods)
	}

	scoreChanged := schemaTestUnsafeAccessorRec{
		Name: "alice",
		SchemaTestUnsafeEmbeddedIndexed: SchemaTestUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 4,
			Tags:  []string{"x", "y"},
			Count: &sameCount,
		},
	}
	mods = mods[:0]
	nextPtr = unsafe.Pointer(&scoreChanged)
	for i := range rt.Indexed {
		if rt.Indexed[i].Modified(basePtr, nextPtr) {
			mods = append(mods, rt.Indexed[i].Name)
		}
	}
	if !slices.Equal(mods, []string{"score"}) {
		t.Fatalf("expected only score to change, got %v", mods)
	}

	tagsChanged := schemaTestUnsafeAccessorRec{
		Name: "alice",
		SchemaTestUnsafeEmbeddedIndexed: SchemaTestUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 3,
			Tags:  []string{"x", "z"},
			Count: &sameCount,
		},
	}
	mods = mods[:0]
	nextPtr = unsafe.Pointer(&tagsChanged)
	for i := range rt.Indexed {
		if rt.Indexed[i].Modified(basePtr, nextPtr) {
			mods = append(mods, rt.Indexed[i].Name)
		}
	}
	if !slices.Equal(mods, []string{"tags"}) {
		t.Fatalf("expected only tags to change, got %v", mods)
	}
}

type schemaTestStringValidationRec struct {
	Name   string `db:"name" rbi:"index"`
	Active bool   `db:"active" rbi:"index"`
	Tags   []int  `db:"tags" rbi:"index"`
}

func TestStringValidationUsesRealStringAccessors(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestStringValidationRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(rt.StringValidation) != 1 || rt.StringValidation[0].Name != "name" {
		t.Fatalf("StringValidation=%+v, want only name", rt.StringValidation)
	}
	rec := schemaTestStringValidationRec{Name: strings.Repeat("x", indexdata.FieldStringRefMax+1)}
	fieldErr := rt.StringValidation[0].Validate(unsafe.Pointer(&rec))
	if fieldErr == nil || !strings.Contains(fieldErr.Error(), "exceeds limit") {
		t.Fatalf("Validate err=%v, want indexed string limit error", fieldErr)
	}
}

type schemaTestStringValidationSlicesRec struct {
	Tags   []string       `db:"tags" rbi:"index"`
	Values []schemaTestVI `db:"values" rbi:"index"`
}

func TestStringValidationChecksSlices(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestStringValidationSlicesRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(rt.StringValidation) != 2 {
		t.Fatalf("StringValidation len=%d want 2", len(rt.StringValidation))
	}
	long := strings.Repeat("x", indexdata.FieldStringRefMax+1)
	rec := schemaTestStringValidationSlicesRec{Tags: []string{"ok", long}, Values: []schemaTestVI{"ok"}}
	if err = rt.IndexedByName["tags"].Validate(unsafe.Pointer(&rec)); err == nil || !strings.Contains(err.Error(), `field "tags"`) {
		t.Fatalf("tags validation err=%v, want field tags limit error", err)
	}
	rec = schemaTestStringValidationSlicesRec{Tags: []string{"ok"}, Values: []schemaTestVI{schemaTestVI(long)}}
	if err = rt.IndexedByName["values"].Validate(unsafe.Pointer(&rec)); err == nil || !strings.Contains(err.Error(), `field "values"`) {
		t.Fatalf("values validation err=%v, want field values limit error", err)
	}
}

type schemaTestMeasureRec struct {
	Signed   int64    `db:"signed" rbi:"measure"`
	Unsigned uint64   `db:"unsigned" rbi:"measure"`
	Float    float64  `db:"float" rbi:"measure"`
	Float32  float32  `db:"float32" rbi:"measure"`
	Ptr      *uint64  `db:"ptr" rbi:"measure"`
	FloatPtr *float64 `db:"float_ptr" rbi:"measure"`
	Invalid  string   `db:"invalid"`
	Indexed  []string `db:"indexed" rbi:"index"`
}

func TestMeasureAccessorsReadAndDetectChanges(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestMeasureRec](), Config{Index: map[string]IndexKind{
		"Signed":   IndexMeasure,
		"Unsigned": IndexMeasure,
		"Float":    IndexMeasure,
		"Float32":  IndexMeasure,
		"Ptr":      IndexMeasure,
		"FloatPtr": IndexMeasure,
	}})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ptr := uint64(9)
	oldRec := schemaTestMeasureRec{Signed: -3, Unsigned: 7, Float: 1.25, Ptr: &ptr}
	newPtr := uint64(10)
	newRec := schemaTestMeasureRec{Signed: -4, Unsigned: 8, Float: 1.5, Ptr: &newPtr}
	oldPtr := unsafe.Pointer(&oldRec)
	newPtrUnsafe := unsafe.Pointer(&newRec)

	if got, ok := rt.MeasuresByName["signed"].Read(oldPtr); !ok || got != uint64(oldRec.Signed) {
		t.Fatalf("signed read=(%d,%v)", got, ok)
	}
	if got, ok := rt.MeasuresByName["unsigned"].Read(oldPtr); !ok || got != 7 {
		t.Fatalf("unsigned read=(%d,%v)", got, ok)
	}
	if got, ok := rt.MeasuresByName["float"].Read(oldPtr); !ok || got != math.Float64bits(1.25) {
		t.Fatalf("float read=(%d,%v)", got, ok)
	}
	if got, ok := rt.MeasuresByName["float32"].Read(oldPtr); !ok || got != math.Float64bits(0) {
		t.Fatalf("float32 read=(%d,%v)", got, ok)
	}
	if got, ok := rt.MeasuresByName["ptr"].Read(oldPtr); !ok || got != 9 {
		t.Fatalf("ptr read=(%d,%v)", got, ok)
	}
	if !rt.MeasuresByName["signed"].Modified(oldPtr, newPtrUnsafe) {
		t.Fatal("signed change was not detected")
	}
	if !rt.MeasuresByName["ptr"].Modified(oldPtr, newPtrUnsafe) {
		t.Fatal("ptr change was not detected")
	}
	negZero := math.Copysign(0, -1)
	posZero := 0.0
	oldZero := schemaTestMeasureRec{Float: negZero, Float32: float32(negZero), FloatPtr: &negZero}
	newZero := schemaTestMeasureRec{Float: posZero, Float32: float32(posZero), FloatPtr: &posZero}
	oldZeroPtr := unsafe.Pointer(&oldZero)
	newZeroPtr := unsafe.Pointer(&newZero)
	if rt.MeasuresByName["float"].Modified(oldZeroPtr, newZeroPtr) {
		t.Fatal("float -0/+0 canonical match was reported as modified")
	}
	if rt.MeasuresByName["float32"].Modified(oldZeroPtr, newZeroPtr) {
		t.Fatal("float32 -0/+0 canonical match was reported as modified")
	}
	if rt.MeasuresByName["float_ptr"].Modified(oldZeroPtr, newZeroPtr) {
		t.Fatal("float pointer -0/+0 canonical match was reported as modified")
	}
	if got, ok := rt.MeasuresByName["float"].Read(oldZeroPtr); !ok || got != math.Float64bits(0) {
		t.Fatalf("float -0 read=(%x,%v), want +0 bits", got, ok)
	}

	oldNaN := math.Float64frombits(0x7ff0000000000001)
	newNaN := math.Float64frombits(0x7ff8000000000001)
	oldNaNRec := schemaTestMeasureRec{Float: oldNaN, Float32: math.Float32frombits(0x7f800001), FloatPtr: &oldNaN}
	newNaNRec := schemaTestMeasureRec{Float: newNaN, Float32: math.Float32frombits(0x7fc00001), FloatPtr: &newNaN}
	oldNaNPtr := unsafe.Pointer(&oldNaNRec)
	newNaNPtr := unsafe.Pointer(&newNaNRec)
	if rt.MeasuresByName["float"].Modified(oldNaNPtr, newNaNPtr) {
		t.Fatal("float NaN canonical match was reported as modified")
	}
	if rt.MeasuresByName["float32"].Modified(oldNaNPtr, newNaNPtr) {
		t.Fatal("float32 NaN canonical match was reported as modified")
	}
	if rt.MeasuresByName["float_ptr"].Modified(oldNaNPtr, newNaNPtr) {
		t.Fatal("float pointer NaN canonical match was reported as modified")
	}
	if got, ok := rt.MeasuresByName["float"].Read(oldNaNPtr); !ok || got != keycodec.CanonicalFloat64NaNBits {
		t.Fatalf("float NaN read=(%x,%v), want canonical NaN bits", got, ok)
	}

	if _, err = Compile(reflect.TypeFor[schemaTestMeasureRec](), Config{Index: map[string]IndexKind{"Invalid": IndexMeasure}}); err == nil || !strings.Contains(err.Error(), "measure field Invalid has unsupported type") {
		t.Fatalf("invalid measure err=%v", err)
	}
}

type schemaTestNamedStrings []string

type schemaTestPatchNested struct {
	Values []int
}

type schemaTestPatchRec struct {
	Scalar int64
	Typed  []string
	Named  schemaTestNamedStrings
	Nested schemaTestPatchNested
	VI     schemaTestVI
	VIPtr  *schemaTestVI
}

type SchemaTestPatchApplyEmbedded struct {
	Embedded string `db:"embedded_db" json:"embeddedJSON"`
}

type schemaTestPatchApplyRec struct {
	SchemaTestPatchApplyEmbedded
	GoName    string
	DBName    string `db:"db_name"`
	JSONName  string `json:"jsonName"`
	Ptr       *int
	PtrTags   *[]int16
	PtrPtr    **int
	PtrAny    *any
	StringPtr *string
	Strings   []string
	Tags      []int16
	Named     schemaTestNamedStrings
	Nested    schemaTestPatchNested
	Any       any
	Anys      []any
	I8        int8
	U8        uint8
	UPtr      uintptr
	F32       float32
	VI        schemaTestVI
	VIPtr     *schemaTestVI
}

type schemaTestNamedIntPtr *int

type schemaTestNamedPointerApplyRec struct {
	Ptr schemaTestNamedIntPtr
	Any any
}

func TestPatchAccessorsEqualCopyAndOrdinalCopies(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestPatchRec](), Config{Index: map[string]IndexKind{"Scalar": IndexDefault, "VI": IndexUnique}})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldVI := schemaTestVI("AA")
	newVI := schemaTestVI("BB")
	oldRec := schemaTestPatchRec{Scalar: 1, Typed: []string{"a"}, Named: schemaTestNamedStrings{"n1"}, Nested: schemaTestPatchNested{Values: []int{1}}, VI: "AA", VIPtr: &oldVI}
	newRec := schemaTestPatchRec{Scalar: 2, Typed: []string{"b"}, Named: schemaTestNamedStrings{"n2"}, Nested: schemaTestPatchNested{Values: []int{2}}, VI: "BB", VIPtr: &newVI}
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)

	scalar := schemaTestPatchAccess(t, rt, "Scalar")
	if scalar.ValueEqual == nil || scalar.ValueEqual(oldPtr, newPtr) {
		t.Fatal("scalar patch equality failed")
	}
	if scalar.CopyValue == nil || scalar.CopyValue(newPtr).(int64) != 2 {
		t.Fatal("scalar patch copy failed")
	}

	typed := schemaTestPatchAccess(t, rt, "Typed")
	copiedTyped := typed.CopyValue(newPtr).([]string)
	copiedTyped[0] = "mutated"
	if newRec.Typed[0] != "b" {
		t.Fatal("typed slice copy aliases source")
	}

	named := schemaTestPatchAccess(t, rt, "Named")
	copiedNamed := named.CopyValue(newPtr).(schemaTestNamedStrings)
	copiedNamed[0] = "mutated"
	if newRec.Named[0] != "n2" {
		t.Fatal("named immutable slice copy aliases source")
	}

	nested := schemaTestPatchAccess(t, rt, "Nested")
	if nested.ValueEqual != nil || nested.CopyValue != nil {
		t.Fatal("mutable nested patch field must use root fallback")
	}
	vi := schemaTestPatchAccess(t, rt, "VI")
	if vi.ValueEqual == nil || vi.ValueEqual(oldPtr, newPtr) {
		t.Fatal("ValueIndexer scalar patch equality failed")
	}
	if vi.CopyValue != nil {
		t.Fatal("ValueIndexer copy should use root fallback")
	}
	viPtr := schemaTestPatchAccess(t, rt, "VIPtr")
	if viPtr.ValueEqual == nil || viPtr.ValueEqual(oldPtr, newPtr) {
		t.Fatal("ValueIndexer pointer patch equality failed")
	}
	newRec.VIPtr = &oldVI
	if !viPtr.ValueEqual(oldPtr, newPtr) {
		t.Fatal("ValueIndexer pointer patch equality should compare pointed value")
	}
	oldRec.VIPtr = nil
	newRec.VIPtr = nil
	if !viPtr.ValueEqual(oldPtr, newPtr) {
		t.Fatal("ValueIndexer pointer patch equality should treat nil pointers as equal")
	}
	newRec.VIPtr = &newVI
	if viPtr.ValueEqual(oldPtr, newPtr) {
		t.Fatal("ValueIndexer pointer patch equality should detect nil/non-nil change")
	}

	for _, acc := range rt.Indexed {
		if rt.IndexedByName[acc.Name].PatchOrdinal != acc.PatchOrdinal {
			t.Fatalf("IndexedByName PatchOrdinal mismatch for %s", acc.Name)
		}
	}
	for _, acc := range rt.StringValidation {
		if rt.IndexedByName[acc.Name].PatchOrdinal != acc.PatchOrdinal {
			t.Fatalf("StringValidation PatchOrdinal mismatch for %s", acc.Name)
		}
	}
	for _, acc := range rt.Unique {
		if rt.IndexedByName[acc.Name].PatchOrdinal != acc.PatchOrdinal {
			t.Fatalf("Unique PatchOrdinal mismatch for %s", acc.Name)
		}
	}
}

func schemaTestPatchAccess(t *testing.T, rt *Schema, name string) PatchFieldAccessor {
	t.Helper()
	for i := range rt.Patch.Access {
		acc := rt.Patch.Access[i]
		if acc.Field.Name == name {
			return acc
		}
	}
	t.Fatalf("missing patch accessor %q", name)
	return PatchFieldAccessor{}
}

func TestPatchRuntimeApplyNamesAndConversions(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestPatchApplyRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	vi := schemaTestVI("BB")
	anyPtr := 77
	var nilAnyPtr *int
	ptrTags := []int16{3, 4}
	ptrPtrValue := 88
	ptrPtr := &ptrPtrValue
	ptrAnyValue := any(99)
	rec := schemaTestPatchApplyRec{}
	err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{
		{Name: "GoName", Value: "go"},
		{Name: "db_name", Value: "db"},
		{Name: "jsonName", Value: "json"},
		{Name: "embedded_db", Value: "embedded"},
		{Name: "Ptr", Value: int64(42)},
		{Name: "PtrTags", Value: &ptrTags},
		{Name: "PtrPtr", Value: &ptrPtr},
		{Name: "PtrAny", Value: &ptrAnyValue},
		{Name: "Tags", Value: []int{1, 2}},
		{Name: "Named", Value: []string{"n1", "n2"}},
		{Name: "Nested", Value: schemaTestPatchNested{Values: []int{7, 8}}},
		{Name: "Any", Value: &anyPtr},
		{Name: "Anys", Value: []any{&anyPtr, nilAnyPtr}},
		{Name: "I8", Value: int64(12)},
		{Name: "U8", Value: float64(13)},
		{Name: "UPtr", Value: uint64(15)},
		{Name: "F32", Value: uint64(14)},
		{Name: "VI", Value: "aa"},
		{Name: "VIPtr", Value: vi},
	}, false)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if rec.GoName != "go" || rec.DBName != "db" || rec.JSONName != "json" || rec.Embedded != "embedded" {
		t.Fatalf("name resolution failed: %+v", rec)
	}
	if rec.Ptr == nil || *rec.Ptr != 42 {
		t.Fatalf("pointer conversion failed: %+v", rec.Ptr)
	}
	if rec.PtrTags == nil || !slices.Equal(*rec.PtrTags, []int16{3, 4}) {
		t.Fatalf("same-type pointer-to-slice assignment failed: %#v", rec.PtrTags)
	}
	if rec.PtrPtr == nil || *rec.PtrPtr == nil || **rec.PtrPtr != 88 {
		t.Fatalf("same-type nested pointer assignment failed: %#v", rec.PtrPtr)
	}
	if rec.PtrAny == nil {
		t.Fatal("same-type pointer-to-interface assignment produced nil pointer")
	}
	if got, ok := (*rec.PtrAny).(int); !ok || got != 99 {
		t.Fatalf("same-type pointer-to-interface assignment failed: %#v", *rec.PtrAny)
	}
	if !slices.Equal(rec.Tags, []int16{1, 2}) {
		t.Fatalf("slice numeric conversion failed: %#v", rec.Tags)
	}
	if !slices.Equal(rec.Named, schemaTestNamedStrings{"n1", "n2"}) {
		t.Fatalf("named slice conversion failed: %#v", rec.Named)
	}
	if !slices.Equal(rec.Nested.Values, []int{7, 8}) {
		t.Fatalf("nested field assignment failed: %#v", rec.Nested)
	}
	if got, ok := rec.Any.(*int); !ok || got == nil || *got != 77 {
		t.Fatalf("interface pointer assignment failed: %#v", rec.Any)
	}
	if len(rec.Anys) != 2 {
		t.Fatalf("interface slice length=%d want 2", len(rec.Anys))
	}
	if got, ok := rec.Anys[0].(*int); !ok || got == nil || *got != 77 {
		t.Fatalf("interface slice pointer assignment failed: %#v", rec.Anys)
	}
	if got, ok := rec.Anys[1].(*int); !ok || got != nil {
		t.Fatalf("interface slice typed nil pointer assignment failed: %#v", rec.Anys)
	}
	if rec.I8 != 12 || rec.U8 != 13 || rec.UPtr != 15 || rec.F32 != 14 {
		t.Fatalf("numeric conversion failed: %+v", rec)
	}
	if rec.VI != "aa" || rec.VIPtr == nil || *rec.VIPtr != "BB" {
		t.Fatalf("ValueIndexer assignment failed: VI=%q VIPtr=%v", rec.VI, rec.VIPtr)
	}
}

func TestPatchRuntimeApplyPreservesNamedPointerTypes(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestNamedPointerApplyRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	value := 11
	ptr := schemaTestNamedIntPtr(&value)
	var rec schemaTestNamedPointerApplyRec
	err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{
		{Name: "Ptr", Value: ptr},
		{Name: "Any", Value: ptr},
	}, false)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if rec.Ptr == nil || *rec.Ptr != 11 {
		t.Fatalf("named pointer assignment failed: %#v", rec.Ptr)
	}
	if rec.Ptr == ptr {
		t.Fatal("named pointer assignment aliases source")
	}
	if got, ok := rec.Any.(schemaTestNamedIntPtr); !ok || got == nil || *got != 11 {
		t.Fatalf("interface named pointer assignment failed: %#v", rec.Any)
	}
}

func TestPatchRuntimeApplyRejectsNumericStringConversions(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestPatchApplyRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	ptr := "old"
	rec := schemaTestPatchApplyRec{
		GoName:    "old",
		StringPtr: &ptr,
		Strings:   []string{"old"},
		VI:        "old",
	}

	for _, p := range []PatchItem{
		{Name: "GoName", Value: 65},
		{Name: "StringPtr", Value: 65},
		{Name: "Strings", Value: []int{65}},
		{Name: "VI", Value: 65},
	} {
		if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{p}, false); err == nil || !strings.Contains(err.Error(), "type mismatch") {
			t.Fatalf("Apply(%s) error=%v, want type mismatch", p.Name, err)
		}
	}

	if rec.GoName != "old" || rec.StringPtr != &ptr || !slices.Equal(rec.Strings, []string{"old"}) || rec.VI != "old" {
		t.Fatalf("failed numeric string patch changed record: %+v", rec)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "VI", Value: "new"}}, false); err != nil {
		t.Fatalf("Apply named string: %v", err)
	}
	if rec.VI != "new" {
		t.Fatalf("named string patch failed: %q", rec.VI)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{
		{Name: "GoName", Value: []byte("bytes")},
		{Name: "StringPtr", Value: []rune("runes")},
	}, false); err != nil {
		t.Fatalf("Apply byte/rune string conversions: %v", err)
	}
	if rec.GoName != "bytes" || rec.StringPtr == nil || *rec.StringPtr != "runes" {
		t.Fatalf("byte/rune string conversions failed: %+v", rec)
	}
}

func TestPatchRuntimeApplyUnknownNilAndConversionErrors(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestPatchApplyRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	n := 5
	rec := schemaTestPatchApplyRec{GoName: "old", Ptr: &n, Tags: []int16{1}}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "Missing", Value: 1}}, true); err != nil {
		t.Fatalf("Apply ignored unknown: %v", err)
	}
	if rec.GoName != "old" {
		t.Fatalf("ignored unknown changed record: %+v", rec)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "Missing", Value: 1}}, false); err == nil || !strings.Contains(err.Error(), "cannot patch field Missing") {
		t.Fatalf("strict unknown error=%v", err)
	}

	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "Ptr", Value: nil}, {Name: "Tags", Value: nil}}, false); err != nil {
		t.Fatalf("Apply nil nillable: %v", err)
	}
	if rec.Ptr != nil || rec.Tags != nil {
		t.Fatalf("nil nillable assignment failed: %+v", rec)
	}
	rec.Tags = []int16{1}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "Tags", Value: []int16(nil)}}, false); err != nil {
		t.Fatalf("Apply typed nil slice: %v", err)
	}
	if rec.Tags != nil {
		t.Fatalf("typed nil slice assignment failed: %#v", rec.Tags)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "GoName", Value: nil}}, false); err == nil || !strings.Contains(err.Error(), "cannot assign nil to non-nillable field") {
		t.Fatalf("nil non-nillable error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "I8", Value: int64(200)}}, false); err == nil || !strings.Contains(err.Error(), "overflows int field") {
		t.Fatalf("int overflow error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "U8", Value: uintptr(300)}}, false); err == nil || !strings.Contains(err.Error(), "overflows uint field") {
		t.Fatalf("uintptr source overflow error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "UPtr", Value: -1}}, false); err == nil || !strings.Contains(err.Error(), "negative int") {
		t.Fatalf("negative uintptr error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "UPtr", Value: 1.5}}, false); err == nil || !strings.Contains(err.Error(), "cannot assign float") {
		t.Fatalf("fractional uintptr error=%v", err)
	}
	uintptrOverflow := math.Ldexp(1, int(reflect.TypeFor[uintptr]().Bits()))
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "UPtr", Value: uintptrOverflow}}, false); err == nil || !strings.Contains(err.Error(), "overflows uint field") {
		t.Fatalf("uintptr overflow error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "Tags", Value: []any{nil}}}, false); err == nil || !strings.Contains(err.Error(), "cannot set nil to non-pointer slice element") {
		t.Fatalf("slice nil element error=%v", err)
	}
}

func schemaTestIndexViewContains(storage indexdata.FieldStorage, key string, id uint64) bool {
	ids := indexdata.NewFieldIndexViewFromStorage(storage).LookupPostingRetained(key)
	ok := ids.Contains(id)
	ids.Release()
	return ok
}

func schemaTestIndexViewContainsKey(storage indexdata.FieldStorage, key uint64, id uint64) bool {
	ids := indexdata.NewFieldIndexViewFromStorage(storage).LookupPostingRetainedKey(keycodec.FromU64(key))
	ok := ids.Contains(id)
	ids.Release()
	return ok
}

func TestRuntimeLookupHelpers(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	info, ok := rt.IndexedByName.ResolveField("name")
	if !ok || info.Ordinal != rt.IndexedByName["name"].Ordinal {
		t.Fatalf("ResolveField(name)=(%d,%v)", info.Ordinal, ok)
	}
	wantScalarCaps := qir.FieldCapNilPredicate | qir.FieldCapPosOrder
	if info.Caps != wantScalarCaps {
		t.Fatalf("ResolveField(name) caps=%d want %d", info.Caps, wantScalarCaps)
	}
	info, ok = rt.IndexedByName.ResolveField("tags")
	if !ok || info.Caps != qir.FieldCapAll {
		t.Fatalf("ResolveField(tags) caps=%d ok=%v want %d", info.Caps, ok, qir.FieldCapAll)
	}
	if _, ok = rt.IndexedByName.ResolveField("missing"); ok {
		t.Fatal("ResolveField(missing) succeeded")
	}
	if !rt.PatchNameTouchesUnique("Unique") {
		t.Fatal("unique patch field was not recognized")
	}
	if rt.PatchNameTouchesUnique("Name") {
		t.Fatal("non-unique patch field reported unique")
	}
	if !FieldUsesOrderedNumericKeys(rt.Fields["age"]) {
		t.Fatal("age should use ordered numeric keys")
	}
	if FieldUsesOrderedNumericKeys(rt.Fields["tags"]) {
		t.Fatal("tags should not use ordered numeric keys")
	}
	if unix, ok := QueryValueToUnixSeconds(reflect.ValueOf(time.Unix(77, 123).UTC())); !ok || unix != 77 {
		t.Fatalf("QueryValueToUnixSeconds=(%d,%v)", unix, ok)
	}
	if !reflect.TypeFor[schemaTestVI]().Implements(ValueIndexerType) {
		t.Fatal("schemaTestVI must implement ValueIndexer")
	}
}

func TestUnwrapQueryValuePreservesTypedNilValueIndexer(t *testing.T) {
	var folded *schemaTestPtrFoldedString
	v, isNil := UnwrapQueryValue(reflect.ValueOf(folded))
	if isNil {
		t.Fatal("typed nil ValueIndexer pointer was reported as nil")
	}
	if v.Kind() != reflect.Pointer || !v.IsNil() {
		t.Fatalf("unwrapped value=%v kind=%v want nil pointer", v, v.Kind())
	}
	if got := v.Interface().(ValueIndexer).IndexingValue(); got != "<nil>" {
		t.Fatalf("typed nil ValueIndexer key=%q want %q", got, "<nil>")
	}
}

func TestUnwrapQueryValueTreatsNilValueReceiverValueIndexerPointerAsNil(t *testing.T) {
	var vi *schemaTestVI
	if _, isNil := UnwrapQueryValue(reflect.ValueOf(vi)); !isNil {
		t.Fatal("typed nil pointer to value-receiver ValueIndexer was not reported as nil")
	}
}

func TestBuildFieldStateHotPaths(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Scores: []int64{5, 7, 5},
	}
	ptr := unsafe.Pointer(&rec)

	stringLocal := NewBuildFieldLocalState(false, false, BuildFieldRunTargetEntries)
	rt.IndexedByName["name"].WriteBuild(ptr, BuildSink{State: &stringLocal, Idx: 1})
	stringState := NewBuildFieldState(false)
	stringLocal.FlushRegularInto(stringState)
	stringStorage := stringState.MaterializeStorage()
	defer stringStorage.Release()
	if !schemaTestIndexViewContains(stringStorage, "alice", 1) {
		t.Fatal("string build materialization lost posting")
	}

	fixedLocal := NewBuildFieldLocalState(true, true, BuildFieldRunTargetEntries)
	rt.IndexedByName["scores"].WriteBuild(ptr, BuildSink{State: &fixedLocal, Idx: 2})
	fixedState := NewBuildFieldState(true)
	fixedLocal.FlushAllInto(fixedState)
	fixedStorage := fixedState.MaterializeStorage()
	defer fixedStorage.Release()
	if !schemaTestIndexViewContainsKey(fixedStorage, keycodec.OrderedInt64Key(5), 2) {
		t.Fatal("fixed build materialization lost posting")
	}
	universe := (posting.List{}).BuildAdded(2)
	lenStorage, _ := fixedState.MaterializeLenStorage(universe)
	defer lenStorage.Release()
	if !schemaTestIndexViewContainsKey(lenStorage, 2, 2) {
		t.Fatal("len build materialization lost posting")
	}

	nilLocal := NewBuildFieldLocalState(true, false, BuildFieldRunTargetEntries)
	rt.IndexedByName["maybe"].WriteBuild(ptr, BuildSink{State: &nilLocal, Idx: 3})
	nilState := NewBuildFieldState(false)
	nilLocal.FlushAllInto(nilState)
	nilStorage := nilState.MaterializeNilStorage()
	defer nilStorage.Release()
	if !schemaTestIndexViewContains(nilStorage, indexdata.NilIndexEntryKey, 3) {
		t.Fatal("nil build materialization lost posting")
	}

	flushLocal := NewBuildFieldLocalState(false, false, BuildFieldRunTargetEntries)
	for i := 0; i < BuildFieldRunTargetEntries; i++ {
		flushLocal.addValue(keycodec.U64ByteString(uint64(i)), uint64(i+10))
	}
	if !flushLocal.ShouldFlushRegular() {
		t.Fatal("string local state should be ready to flush")
	}
	flushFixed := NewBuildFieldLocalState(true, false, BuildFieldRunTargetEntries)
	for i := 0; i < BuildFieldRunTargetEntries; i++ {
		flushFixed.addFixedValue(uint64(i), uint64(i+10))
	}
	if !flushFixed.ShouldFlushRegular() {
		t.Fatal("fixed local state should be ready to flush")
	}
}

func TestIndexStateCollectAndMaterialize(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Scores: []int64{5, 7, 5},
	}
	ptr := unsafe.Pointer(&rec)

	var stringState IndexState
	rt.IndexedByName["name"].CollectIndexValue(ptr, 1, &stringState)
	if !stringState.Changed() {
		t.Fatal("string overlay state not marked changed")
	}
	stringStorage := stringState.MaterializeStorage(false)
	defer stringStorage.Release()
	if !schemaTestIndexViewContains(stringStorage, "alice", 1) {
		t.Fatal("string overlay materialization lost posting")
	}

	var fixedState IndexState
	rt.IndexedByName["scores"].CollectIndexValue(ptr, 2, &fixedState)
	fixedStorage := fixedState.MaterializeStorage(true)
	defer fixedStorage.Release()
	if !schemaTestIndexViewContainsKey(fixedStorage, keycodec.OrderedInt64Key(7), 2) {
		t.Fatal("fixed overlay materialization lost posting")
	}
	lenStorage, _ := fixedState.MaterializeLenStorage((posting.List{}).BuildAdded(2))
	defer lenStorage.Release()
	if !schemaTestIndexViewContainsKey(lenStorage, 2, 2) {
		t.Fatal("len overlay materialization lost posting")
	}

	var nilState IndexState
	rt.IndexedByName["maybe"].CollectIndexValue(ptr, 3, &nilState)
	nilStorage := nilState.MaterializeNilStorage()
	defer nilStorage.Release()
	if !schemaTestIndexViewContains(nilStorage, indexdata.NilIndexEntryKey, 3) {
		t.Fatal("nil overlay materialization lost posting")
	}
}

func TestIndexStateMaterializedStorageSurvivesStateRelease(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Tags:   []string{"go", "db"},
		Scores: []int64{5, 7},
	}
	ptr := unsafe.Pointer(&rec)
	states := GetIndexStates(len(rt.Indexed))

	nameOrdinal := rt.IndexedByName["name"].Ordinal
	tagsOrdinal := rt.IndexedByName["tags"].Ordinal
	maybeOrdinal := rt.IndexedByName["maybe"].Ordinal
	rt.IndexedByName["name"].CollectIndexValue(ptr, 1, &states[nameOrdinal])
	rt.IndexedByName["tags"].CollectIndexValue(ptr, 1, &states[tagsOrdinal])
	rt.IndexedByName["maybe"].CollectIndexValue(ptr, 1, &states[maybeOrdinal])

	nameStorage := states[nameOrdinal].MaterializeStorage(false)
	defer nameStorage.Release()
	tagStorage := states[tagsOrdinal].MaterializeStorage(false)
	defer tagStorage.Release()
	nilStorage := states[maybeOrdinal].MaterializeNilStorage()
	defer nilStorage.Release()
	universe := (posting.List{}).BuildAdded(1)
	lenStorage, _ := states[tagsOrdinal].MaterializeLenStorage(universe)
	defer lenStorage.Release()
	defer universe.Release()

	ReleaseIndexStates(states)

	if !schemaTestIndexViewContains(nameStorage, "alice", 1) {
		t.Fatal("materialized scalar storage did not survive index state release")
	}
	if !schemaTestIndexViewContains(tagStorage, "go", 1) || !schemaTestIndexViewContains(tagStorage, "db", 1) {
		t.Fatal("materialized slice storage did not survive index state release")
	}
	if !schemaTestIndexViewContains(nilStorage, indexdata.NilIndexEntryKey, 1) {
		t.Fatal("materialized nil storage did not survive index state release")
	}
	if !schemaTestIndexViewContainsKey(lenStorage, 2, 1) {
		t.Fatal("materialized len storage did not survive index state release")
	}
}

func TestIndexStateReleaseDiscardsUnmaterializedPostingLists(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Scores: []int64{5, 7},
	}
	ptr := unsafe.Pointer(&rec)
	states := GetIndexStates(len(rt.Indexed))

	nameOrdinal := rt.IndexedByName["name"].Ordinal
	scoresOrdinal := rt.IndexedByName["scores"].Ordinal
	maybeOrdinal := rt.IndexedByName["maybe"].Ordinal
	for id := uint64(1); id <= 8; id++ {
		rt.IndexedByName["name"].CollectIndexValue(ptr, id, &states[nameOrdinal])
		rt.IndexedByName["scores"].CollectIndexValue(ptr, id, &states[scoresOrdinal])
		rt.IndexedByName["maybe"].CollectIndexValue(ptr, id, &states[maybeOrdinal])
	}

	ReleaseIndexStates(states)
	states = GetIndexStates(len(rt.Indexed))
	defer ReleaseIndexStates(states)

	if states[nameOrdinal].Changed() || states[nameOrdinal].index != nil {
		t.Fatalf("released string index state was not reset: %+v", states[nameOrdinal])
	}
	if states[scoresOrdinal].Changed() || states[scoresOrdinal].fixed != nil || states[scoresOrdinal].lengths != nil {
		t.Fatalf("released fixed index state was not reset: %+v", states[scoresOrdinal])
	}
	if states[maybeOrdinal].Changed() || states[maybeOrdinal].nils != nil {
		t.Fatalf("released nil index state was not reset: %+v", states[maybeOrdinal])
	}
}

func TestInsertStateCollectMergeResetAndHints(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Scores: []int64{5, 7, 5},
	}
	ptr := unsafe.Pointer(&rec)

	var state InsertState
	rt.IndexedByName["scores"].CollectInsertValue(ptr, 1, false, &state)
	if !state.Changed() {
		t.Fatal("insert state not marked changed")
	}
	storage := rt.IndexedByName["scores"].MergeInsertStorageOwned(indexdata.FieldStorage{}, &state, false)
	defer storage.Release()
	if !schemaTestIndexViewContainsKey(storage, keycodec.OrderedInt64Key(5), 1) {
		t.Fatal("insert merge lost fixed posting")
	}
	if diff := state.LenDiff(); diff == nil {
		t.Fatal("insert state missing len diff")
	}
	state.Reset()
	if state.Changed() {
		t.Fatal("insert reset left changed flag set")
	}
	state.discard()
	if state.Changed() || state.lengths != nil {
		t.Fatal("insert discard kept owned internals")
	}

	states := GetInsertStates(len(rt.Indexed))
	defer ReleaseInsertStates(states)
	prev := make([]indexdata.FieldStorage, len(rt.Indexed))
	nameOrdinal := rt.IndexedByName["name"].Ordinal
	prevMap := indexdata.GetPostingMap()
	prevMap["alice"] = (posting.List{}).BuildAdded(1)
	prev[nameOrdinal] = indexdata.NewRegularFieldStorageFromPostingMapOwned(prevMap)
	defer prev[nameOrdinal].Release()
	InitInsertStateHints(states, rt.Indexed, prev, nil, nil, 4)
	if states[nameOrdinal].indexHint != 4 {
		t.Fatalf("index hint=%d want 4", states[nameOrdinal].indexHint)
	}

	var stringState InsertState
	rt.IndexedByName["name"].CollectInsertValue(ptr, 11, false, &stringState)
	stringStorage := rt.IndexedByName["name"].MergeInsertStorageOwned(indexdata.FieldStorage{}, &stringState, false)
	defer stringStorage.Release()
	if !schemaTestIndexViewContains(stringStorage, "alice", 11) {
		t.Fatal("insert merge lost string posting")
	}
	stringState.discard()

	var nilState InsertState
	rt.IndexedByName["maybe"].CollectInsertValue(ptr, 12, false, &nilState)
	nilStorage := rt.IndexedByName["maybe"].MergeInsertNilStorageOwned(indexdata.FieldStorage{}, &nilState)
	defer nilStorage.Release()
	if !schemaTestIndexViewContains(nilStorage, indexdata.NilIndexEntryKey, 12) {
		t.Fatal("insert merge lost nil posting")
	}
	nilState.discard()
}

func TestInsertStateResetDropsPreviousLogicalEntries(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldRec := schemaTestAccessorRec{Tags: []string{"old"}}
	newRec := schemaTestAccessorRec{Tags: []string{"new", "next"}}

	var state InsertState
	rt.IndexedByName["tags"].CollectInsertValue(unsafe.Pointer(&oldRec), 1, false, &state)
	state.Reset()
	rt.IndexedByName["tags"].CollectInsertValue(unsafe.Pointer(&newRec), 2, false, &state)

	storage := rt.IndexedByName["tags"].MergeInsertStorageOwned(indexdata.FieldStorage{}, &state, false)
	defer storage.Release()
	lenStorage := (indexdata.FieldStorage{}).ApplyLenPostingDiff(state.LenDiff())
	defer lenStorage.Release()
	state.Reset()

	if schemaTestIndexViewContains(storage, "old", 1) {
		t.Fatal("insert state reset kept old string posting")
	}
	if !schemaTestIndexViewContains(storage, "new", 2) || !schemaTestIndexViewContains(storage, "next", 2) {
		t.Fatal("insert state reset lost new string postings")
	}
	if schemaTestIndexViewContainsKey(lenStorage, 1, 1) {
		t.Fatal("insert state reset kept old len posting")
	}
	if !schemaTestIndexViewContainsKey(lenStorage, 2, 2) {
		t.Fatal("insert state reset lost new len posting")
	}
	state.discard()
}

func TestInsertStateSliceCleanupDropsPreviousLogicalEntries(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldRec := schemaTestAccessorRec{Tags: []string{"old"}}
	newRec := schemaTestAccessorRec{Tags: []string{"new", "next"}}
	acc := rt.IndexedByName["tags"]
	states := make([]InsertState, len(rt.Indexed))
	state := &states[acc.Ordinal]

	acc.CollectInsertValue(unsafe.Pointer(&oldRec), 1, false, state)
	cleanupSnapshotFieldInsertStateSlice(states)
	if state.Changed() {
		t.Fatal("insert state cleanup kept changed flag")
	}
	acc.CollectInsertValue(unsafe.Pointer(&newRec), 2, false, state)

	storage := acc.MergeInsertStorageOwned(indexdata.FieldStorage{}, state, false)
	defer storage.Release()
	lenStorage := (indexdata.FieldStorage{}).ApplyLenPostingDiff(state.LenDiff())
	defer lenStorage.Release()
	state.Reset()

	if schemaTestIndexViewContains(storage, "old", 1) {
		t.Fatal("insert state cleanup kept old string posting")
	}
	if !schemaTestIndexViewContains(storage, "new", 2) || !schemaTestIndexViewContains(storage, "next", 2) {
		t.Fatal("insert state cleanup lost new string postings")
	}
	if schemaTestIndexViewContainsKey(lenStorage, 1, 1) {
		t.Fatal("insert state cleanup kept old len posting")
	}
	if !schemaTestIndexViewContainsKey(lenStorage, 2, 2) {
		t.Fatal("insert state cleanup lost new len posting")
	}
	state.discard()
}

func TestBatchStateCollectApplyReset(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	maybe := int64(5)
	oldRec := schemaTestAccessorRec{
		Name:   "alice",
		Age:    -7,
		Maybe:  &maybe,
		Tags:   []string{"go", "db"},
		Scores: []int64{5, 7},
	}
	newRec := schemaTestAccessorRec{
		Name:   "bob",
		Age:    9,
		Tags:   []string{"db", "search"},
		Scores: []int64{7, 9, 11},
	}
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)

	var scalar BatchState
	rt.IndexedByName["name"].CollectBatchDiff(1, oldPtr, newPtr, false, &scalar)
	if !scalar.Changed() {
		t.Fatal("scalar batch state not marked changed")
	}
	scalarStorage := rt.IndexedByName["name"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &scalar, false)
	defer scalarStorage.Release()
	if !schemaTestIndexViewContains(scalarStorage, "bob", 1) {
		t.Fatal("scalar batch apply lost add posting")
	}
	scalar.Reset()
	if scalar.Changed() {
		t.Fatal("scalar batch reset left changed flag set")
	}

	var fixedScalar BatchState
	rt.IndexedByName["age"].CollectBatchDiff(4, oldPtr, newPtr, false, &fixedScalar)
	if !fixedScalar.Changed() {
		t.Fatal("fixed scalar batch state not marked changed")
	}
	fixedScalarStorage := rt.IndexedByName["age"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &fixedScalar, false)
	defer fixedScalarStorage.Release()
	if !schemaTestIndexViewContainsKey(fixedScalarStorage, keycodec.OrderedInt64Key(9), 4) {
		t.Fatal("fixed scalar batch apply lost add posting")
	}
	fixedScalar.Reset()

	var stringSlice BatchState
	rt.IndexedByName["tags"].CollectBatchDiff(5, oldPtr, newPtr, false, &stringSlice)
	if !stringSlice.Changed() {
		t.Fatal("string slice batch state not marked changed")
	}
	stringSliceStorage := rt.IndexedByName["tags"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &stringSlice, false)
	defer stringSliceStorage.Release()
	if !schemaTestIndexViewContains(stringSliceStorage, "search", 5) {
		t.Fatal("string slice batch apply lost add posting")
	}
	stringSlice.Reset()

	var slice BatchState
	rt.IndexedByName["scores"].CollectBatchDiff(2, oldPtr, newPtr, false, &slice)
	if !slice.Changed() {
		t.Fatal("slice batch state not marked changed")
	}
	sliceStorage := rt.IndexedByName["scores"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &slice, false)
	defer sliceStorage.Release()
	if !schemaTestIndexViewContainsKey(sliceStorage, keycodec.OrderedInt64Key(9), 2) {
		t.Fatal("slice batch apply lost fixed add posting")
	}
	if diff := slice.LenDiff(); diff == nil {
		t.Fatal("slice batch state missing len diff")
	}
	slice.discard()
	if slice.Changed() || slice.lengths != nil {
		t.Fatal("batch discard kept owned internals")
	}

	var nilDiff BatchState
	rt.IndexedByName["maybe"].CollectBatchDiff(6, oldPtr, newPtr, false, &nilDiff)
	if !nilDiff.Changed() {
		t.Fatal("nil batch state not marked changed")
	}
	nilStorage := rt.IndexedByName["maybe"].ApplyBatchNilStorageOwned(indexdata.FieldStorage{}, &nilDiff)
	defer nilStorage.Release()
	if !schemaTestIndexViewContains(nilStorage, indexdata.NilIndexEntryKey, 6) {
		t.Fatal("batch nil apply lost nil posting")
	}
	nilDiff.discard()
}

func TestBatchStateResetDropsPreviousLogicalEntries(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldRec := schemaTestAccessorRec{Tags: []string{"old"}}
	newRec := schemaTestAccessorRec{Tags: []string{"old", "stale"}}
	resetOld := schemaTestAccessorRec{Tags: []string{"drop", "other"}}
	resetNew := schemaTestAccessorRec{Tags: []string{"fresh"}}

	var state BatchState
	rt.IndexedByName["tags"].CollectBatchDiff(1, unsafe.Pointer(&oldRec), unsafe.Pointer(&newRec), false, &state)
	state.Reset()
	rt.IndexedByName["tags"].CollectBatchDiff(2, unsafe.Pointer(&resetOld), unsafe.Pointer(&resetNew), false, &state)

	storage := rt.IndexedByName["tags"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &state, false)
	defer storage.Release()
	lenStorage := (indexdata.FieldStorage{}).ApplyLenPostingDiff(state.LenDiff())
	defer lenStorage.Release()
	state.Reset()

	if schemaTestIndexViewContains(storage, "stale", 1) {
		t.Fatal("batch state reset kept old string diff")
	}
	if !schemaTestIndexViewContains(storage, "fresh", 2) {
		t.Fatal("batch state reset lost new string diff")
	}
	if schemaTestIndexViewContainsKey(lenStorage, 2, 1) {
		t.Fatal("batch state reset kept old len diff")
	}
	if !schemaTestIndexViewContainsKey(lenStorage, 1, 2) {
		t.Fatal("batch state reset lost new len diff")
	}
	state.discard()
}

func TestBatchStateSliceCleanupDropsPreviousLogicalEntries(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldRec := schemaTestAccessorRec{Tags: []string{"old"}}
	newRec := schemaTestAccessorRec{Tags: []string{"old", "stale"}}
	resetOld := schemaTestAccessorRec{Tags: []string{"drop", "other"}}
	resetNew := schemaTestAccessorRec{Tags: []string{"fresh"}}
	acc := rt.IndexedByName["tags"]
	states := make([]BatchState, len(rt.Indexed))
	state := &states[acc.Ordinal]

	acc.CollectBatchDiff(1, unsafe.Pointer(&oldRec), unsafe.Pointer(&newRec), false, state)
	cleanupSnapshotFieldBatchStateSlice(states)
	if state.Changed() || state.old.ok || state.new.ok || state.old.length != 0 || state.new.length != 0 {
		t.Fatalf("batch state cleanup kept logical state: %+v", state)
	}
	acc.CollectBatchDiff(2, unsafe.Pointer(&resetOld), unsafe.Pointer(&resetNew), false, state)

	storage := acc.ApplyBatchStorageOwned(indexdata.FieldStorage{}, state, false)
	defer storage.Release()
	lenStorage := (indexdata.FieldStorage{}).ApplyLenPostingDiff(state.LenDiff())
	defer lenStorage.Release()
	state.Reset()

	if schemaTestIndexViewContains(storage, "stale", 1) {
		t.Fatal("batch state cleanup kept old string diff")
	}
	if !schemaTestIndexViewContains(storage, "fresh", 2) {
		t.Fatal("batch state cleanup lost new string diff")
	}
	if schemaTestIndexViewContainsKey(lenStorage, 2, 1) {
		t.Fatal("batch state cleanup kept old len diff")
	}
	if !schemaTestIndexViewContainsKey(lenStorage, 1, 2) {
		t.Fatal("batch state cleanup lost new len diff")
	}
	state.discard()
}

func TestWriteScratchResetAndDiscardClearLogicalState(t *testing.T) {
	var scratch WriteScratch
	scratch.addString("old")
	scratch.addFixed(7)
	scratch.setLen(3)
	scratch.setNil()
	scratch.reset()
	if scratch.ok || scratch.isNil || scratch.length != 0 || len(scratch.strings) != 0 || len(scratch.fixed) != 0 {
		t.Fatalf("reset scratch kept logical state: %+v", scratch)
	}
	scratch.addString("next")
	scratch.addFixed(9)
	scratch.setLen(2)
	scratch.discard()
	if scratch.ok || scratch.isNil || scratch.length != 0 || scratch.strings != nil || scratch.fixed != nil {
		t.Fatalf("discard scratch kept owned buffers: %+v", scratch)
	}
}

func TestBatchStateReorderedSliceValuesProduceNoDeltas(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldRec := schemaTestAccessorRec{
		Tags:   []string{"go", "db", "go"},
		Scores: []int64{3, 1, 3},
	}
	newRec := schemaTestAccessorRec{
		Tags:   []string{"db", "go"},
		Scores: []int64{1, 3},
	}
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)

	for _, name := range []string{"tags", "scores"} {
		var state BatchState
		rt.IndexedByName[name].CollectBatchDiff(1, oldPtr, newPtr, false, &state)
		if state.Changed() {
			state.Reset()
			t.Fatalf("%s: reorder-only diff unexpectedly marked field as changed", name)
		}
		state.Reset()
	}
}

func TestBatchStateSlicePoolLifecycle(t *testing.T) {
	states := GetBatchStates(3)
	if len(states) != 3 {
		t.Fatalf("GetBatchStates len=%d want 3", len(states))
	}
	states[0].changed = true
	ReleaseBatchStates(states)
}
