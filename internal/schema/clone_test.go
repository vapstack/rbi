package schema

import (
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
	"unsafe"
)

type schemaTestHiddenRuntimeState struct {
	mu   sync.Mutex
	Name string
}

type schemaTestHiddenRuntimeStateRec struct {
	State schemaTestHiddenRuntimeState
}

type schemaTestHiddenMutableBytes struct {
	buf []byte
}

type schemaTestHiddenMutableBytesRec struct {
	Value schemaTestHiddenMutableBytes
}

type schemaTestHiddenScalar struct {
	id   int
	Name string
}

type schemaTestHiddenScalarRec struct {
	Value schemaTestHiddenScalar
}

type schemaTestUnexportedRuntimeStateRec struct {
	Name string
	mu   sync.Mutex
}

type schemaTestCloneNamedStrings []string

type schemaTestCloneNamedCounters map[uint64]int

type schemaTestCloneChild struct {
	Label  string
	Values []int
}

type schemaTestCloneNested struct {
	Label  string
	Tags   []string
	Counts map[string]int
	Child  *schemaTestCloneChild
}

type schemaTestCloneRec struct {
	Name     string
	Ptr      *int
	Tags     []string
	Named    schemaTestCloneNamedStrings
	Times    []time.Time
	Nested   schemaTestCloneNested
	Items    []schemaTestCloneNested
	Lookup   map[string]int
	Counters map[uint64]int
	NamedMap schemaTestCloneNamedCounters
	Children map[string]schemaTestCloneChild
	Fixed    [1]schemaTestCloneNested
}

type schemaTestCloneHiddenValue struct {
	Name   string
	hidden []byte
	mu     sync.Mutex
}

type schemaTestCloneHiddenRec struct {
	Name   string
	Value  schemaTestCloneHiddenValue
	Ptr    *schemaTestCloneHiddenValue
	Items  []schemaTestCloneHiddenValue
	Lookup map[string]schemaTestCloneHiddenValue
	hidden []byte
}

type schemaTestCloneRawSpanRec struct {
	ID     uint64
	Name   string
	At     time.Time
	Names  [2]string
	Times  [1]time.Time
	Counts [2]uint64
}

func TestCompileAllowsHiddenScalarFields(t *testing.T) {
	if _, err := Compile(reflect.TypeFor[schemaTestHiddenScalarRec](), Config{}); err != nil {
		t.Fatalf("Compile hidden scalar field: %v", err)
	}
}

func TestCompileAllowsTopLevelUnexportedRuntimeState(t *testing.T) {
	if _, err := Compile(reflect.TypeFor[schemaTestUnexportedRuntimeStateRec](), Config{}); err != nil {
		t.Fatalf("Compile top-level unexported runtime state: %v", err)
	}
}

func TestCompileIgnoresNestedUnexportedRuntimeState(t *testing.T) {
	if _, err := Compile(reflect.TypeFor[schemaTestHiddenRuntimeStateRec](), Config{}); err != nil {
		t.Fatalf("Compile nested unexported runtime state: %v", err)
	}
	if _, err := Compile(reflect.TypeFor[schemaTestHiddenMutableBytesRec](), Config{}); err != nil {
		t.Fatalf("Compile nested unexported mutable bytes: %v", err)
	}
}

func TestCloneRuntimeDetachesSupportedReferences(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestCloneRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	ptr := 7
	src := schemaTestCloneRec{
		Name:  "root",
		Ptr:   &ptr,
		Tags:  []string{"a", "b"},
		Named: schemaTestCloneNamedStrings{"n1", "n2"},
		Times: []time.Time{time.Unix(10, 0).UTC()},
		Nested: schemaTestCloneNested{
			Label:  "nested",
			Tags:   []string{"nested"},
			Counts: map[string]int{"x": 1},
			Child:  &schemaTestCloneChild{Label: "child", Values: []int{1, 2}},
		},
		Items: []schemaTestCloneNested{{
			Label:  "item",
			Tags:   []string{"item"},
			Counts: map[string]int{"i": 1},
			Child:  &schemaTestCloneChild{Label: "item-child", Values: []int{3}},
		}},
		Lookup: map[string]int{"k": 1},
		Counters: map[uint64]int{
			10: 1,
		},
		NamedMap: schemaTestCloneNamedCounters{
			20: 2,
		},
		Children: map[string]schemaTestCloneChild{
			"c": {Label: "map-child", Values: []int{4}},
		},
		Fixed: [1]schemaTestCloneNested{{
			Label:  "fixed",
			Tags:   []string{"fixed"},
			Counts: map[string]int{"f": 1},
			Child:  &schemaTestCloneChild{Label: "fixed-child", Values: []int{5}},
		}},
	}
	var dst schemaTestCloneRec
	rt.Clone.CloneInto(unsafe.Pointer(&src), unsafe.Pointer(&dst))

	src.Name = "mutated"
	*src.Ptr = 70
	src.Tags[0] = "mutated"
	src.Named[0] = "mutated"
	src.Times[0] = time.Unix(20, 0).UTC()
	src.Nested.Label = "mutated"
	src.Nested.Tags[0] = "mutated"
	src.Nested.Counts["x"] = 9
	src.Nested.Child.Label = "mutated"
	src.Nested.Child.Values[0] = 9
	src.Items[0].Label = "mutated"
	src.Items[0].Tags[0] = "mutated"
	src.Items[0].Counts["i"] = 9
	src.Items[0].Child.Values[0] = 9
	src.Lookup["k"] = 9
	src.Counters[10] = 9
	src.NamedMap[20] = 9
	child := src.Children["c"]
	child.Values[0] = 9
	src.Children["c"] = child
	src.Fixed[0].Label = "mutated"
	src.Fixed[0].Tags[0] = "mutated"
	src.Fixed[0].Counts["f"] = 9
	src.Fixed[0].Child.Values[0] = 9

	if dst.Name != "root" {
		t.Fatalf("scalar clone failed: %q", dst.Name)
	}
	if dst.Ptr == src.Ptr || *dst.Ptr != 7 {
		t.Fatalf("pointer clone aliased source: %#v", dst.Ptr)
	}
	if !slices.Equal(dst.Tags, []string{"a", "b"}) || !slices.Equal(dst.Named, schemaTestCloneNamedStrings{"n1", "n2"}) {
		t.Fatalf("slice clone aliased source: tags=%v named=%v", dst.Tags, dst.Named)
	}
	if !slices.Equal(dst.Times, []time.Time{time.Unix(10, 0).UTC()}) {
		t.Fatalf("time slice clone aliased source: %#v", dst.Times)
	}
	if dst.Nested.Label != "nested" || !slices.Equal(dst.Nested.Tags, []string{"nested"}) || dst.Nested.Counts["x"] != 1 ||
		dst.Nested.Child.Label != "child" || !slices.Equal(dst.Nested.Child.Values, []int{1, 2}) {
		t.Fatalf("nested clone aliased source: %#v", dst.Nested)
	}
	if dst.Items[0].Label != "item" || !slices.Equal(dst.Items[0].Tags, []string{"item"}) || dst.Items[0].Counts["i"] != 1 ||
		!slices.Equal(dst.Items[0].Child.Values, []int{3}) {
		t.Fatalf("slice element clone aliased source: %#v", dst.Items)
	}
	if dst.Lookup["k"] != 1 || dst.Counters[10] != 1 || dst.NamedMap[20] != 2 || !slices.Equal(dst.Children["c"].Values, []int{4}) {
		t.Fatalf("map clone aliased source: lookup=%v counters=%v named=%v children=%v", dst.Lookup, dst.Counters, dst.NamedMap, dst.Children)
	}
	if dst.Fixed[0].Label != "fixed" || !slices.Equal(dst.Fixed[0].Tags, []string{"fixed"}) || dst.Fixed[0].Counts["f"] != 1 ||
		!slices.Equal(dst.Fixed[0].Child.Values, []int{5}) {
		t.Fatalf("array clone aliased source: %#v", dst.Fixed)
	}
}

func TestCloneRuntimeIgnoresUnexportedFields(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestCloneHiddenRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	src := schemaTestCloneHiddenRec{
		Name:   "root",
		Value:  schemaTestCloneHiddenValue{Name: "value", hidden: []byte("hidden")},
		Ptr:    &schemaTestCloneHiddenValue{Name: "ptr", hidden: []byte("hidden")},
		Items:  []schemaTestCloneHiddenValue{{Name: "item", hidden: []byte("hidden")}},
		Lookup: map[string]schemaTestCloneHiddenValue{"k": {Name: "map", hidden: []byte("hidden")}},
		hidden: []byte("root-hidden"),
	}
	var dst schemaTestCloneHiddenRec
	rt.Clone.CloneInto(unsafe.Pointer(&src), unsafe.Pointer(&dst))

	if dst.Name != "root" || dst.Value.Name != "value" || dst.Ptr == nil || dst.Ptr.Name != "ptr" ||
		len(dst.Items) != 1 || dst.Items[0].Name != "item" || dst.Lookup["k"].Name != "map" {
		t.Fatalf("exported clone failed: %#v", &dst)
	}
	if dst.hidden != nil || dst.Value.hidden != nil || dst.Ptr.hidden != nil ||
		dst.Items[0].hidden != nil || dst.Lookup["k"].hidden != nil {
		t.Fatalf("unexported fields were cloned: %#v", &dst)
	}
}

func TestCloneRuntimeRawSpansExcludePointerBearingFields(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestCloneRawSpanRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	typ := reflect.TypeFor[schemaTestCloneRawSpanRec]()
	for _, name := range []string{"Name", "At", "Names", "Times"} {
		field, ok := typ.FieldByName(name)
		if !ok {
			t.Fatalf("field %s not found", name)
		}
		for i := range rt.Clone.spans {
			if byteSpansOverlap(rt.Clone.spans[i].off, rt.Clone.spans[i].size, field.Offset, field.Type.Size()) {
				t.Fatalf("raw clone span overlaps pointer-bearing field %s: span=%+v fieldOff=%d fieldSize=%d", name, rt.Clone.spans[i], field.Offset, field.Type.Size())
			}
		}
	}

	src := schemaTestCloneRawSpanRec{
		ID:     10,
		Name:   "name",
		At:     time.Unix(100, 200).UTC(),
		Names:  [2]string{"left", "right"},
		Times:  [1]time.Time{time.Unix(300, 400).UTC()},
		Counts: [2]uint64{1, 2},
	}
	var dst schemaTestCloneRawSpanRec
	rt.Clone.CloneInto(unsafe.Pointer(&src), unsafe.Pointer(&dst))
	if dst != src {
		t.Fatalf("clone mismatch:\ngot  %#v\nwant %#v", dst, src)
	}
}

func byteSpansOverlap(aOff uintptr, aSize uintptr, bOff uintptr, bSize uintptr) bool {
	return aOff < bOff+bSize && bOff < aOff+aSize
}
