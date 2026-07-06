package schema

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"
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

type schemaTestNamedTimeRec struct {
	When schemaTestNamedTime `db:"when" rbi:"index"`
}

type schemaTestNamedTimePtrRec struct {
	When *schemaTestNamedTime `db:"when" rbi:"index"`
}

type schemaTestNamedTimeSliceVI []schemaTestNamedTime

func (v schemaTestNamedTimeSliceVI) IndexingValue() string {
	return ""
}

type schemaTestNamedTimeSliceVIRec struct {
	When schemaTestNamedTimeSliceVI `db:"when" rbi:"index"`
}

type schemaTestPtrReceiverNamedTimeVI time.Time

func (v *schemaTestPtrReceiverNamedTimeVI) IndexingValue() string {
	if v == nil {
		return "<nil>"
	}
	return time.Time(*v).UTC().Format(time.RFC3339Nano)
}

type schemaTestPtrReceiverNamedTimeVIRec struct {
	When *schemaTestPtrReceiverNamedTimeVI `db:"when" rbi:"index"`
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

func TestCompileRejectsShadowedJSONOnlyCodecFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
	}{
		{name: "index", typ: reflect.TypeFor[schemaTestShadowedJSONOnlyIndexedRec]()},
		{name: "measure", typ: reflect.TypeFor[schemaTestShadowedJSONOnlyMeasureRec]()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(tc.typ, Config{})
			if err == nil || !strings.Contains(err.Error(), "shadowed by direct field") {
				t.Fatalf("Compile err=%v want shadowed codec field error", err)
			}
		})
	}
}

func TestCompileRejectsSameNamePromotedCodecFields(t *testing.T) {
	if _, err := Compile(reflect.TypeFor[schemaTestPatchSameNamePromotedRec](), Config{}); err == nil || !strings.Contains(err.Error(), "ambiguous promoted field") {
		t.Fatalf("Compile err=%v want ambiguous promoted field", err)
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

	for _, typ := range []reflect.Type{
		reflect.TypeFor[schemaTestNamedTimeRec](),
		reflect.TypeFor[schemaTestNamedTimePtrRec](),
		reflect.TypeFor[schemaTestNamedTimeSliceVIRec](),
		reflect.TypeFor[schemaTestPtrReceiverNamedTimeVIRec](),
	} {
		if _, err = Compile(typ, Config{}); err == nil {
			t.Fatalf("Compile(%v) err=nil want unsupported named time wrapper", typ)
		} else if msg := err.Error(); !strings.Contains(msg, "named time field") || !strings.Contains(msg, "is not supported") {
			t.Fatalf("Compile(%v) err=%v want unsupported named time error", typ, err)
		}
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
	if _, err = Compile(reflect.TypeFor[schemaTestUintptrSliceIndexRec](), Config{}); err == nil || !strings.Contains(err.Error(), "unsupported record field") || !strings.Contains(err.Error(), "uintptr") {
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

	if _, err = Compile(reflect.TypeFor[schemaTestRecursivePointerEmbeddedRec](), Config{}); err == nil || !strings.Contains(err.Error(), "recursive record field") {
		t.Fatalf("Compile recursive pointer embedded err=%v", err)
	}

	_, err = Compile(reflect.TypeFor[schemaTestRecursivePointerEmbeddedTaggedRec](), Config{})
	if err == nil || !strings.Contains(err.Error(), "recursive record field") {
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
