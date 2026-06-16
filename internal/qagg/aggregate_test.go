package qagg

import (
	"reflect"
	"strings"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
)

func TestAggregateGroupOrdinalMapResetClearsLogicalEntries(t *testing.T) {
	var m aggregateGroupOrdinalMap
	m.init(8)
	m.put(42, 7)
	if got := m.get(42); got != 7 {
		t.Fatalf("group ordinal before reset=%d want 7", got)
	}

	m.reset()
	m.init(8)
	if got := m.get(42); got != 0 {
		t.Fatalf("group ordinal after reset=%d want 0", got)
	}
}

func TestAggregateGroupOrdinalMapResetDropsOversizedStorage(t *testing.T) {
	var m aggregateGroupOrdinalMap
	m.init(aggregateGroupOrdinalMapPoolMaxCap + 1)
	m.put(42, 7)
	m.reset()

	if m.keys != nil || m.values != nil || m.mask != 0 {
		t.Fatalf("oversized group map storage was retained")
	}
}

func TestAggregateQueryReleaseClearsOwnedPlannerSlices(t *testing.T) {
	q := &Query{
		groups: []aggregateFieldRef{
			{name: "group_a", out: "ga", ordinary: schema.IndexedFieldAccessor{Name: "group_a", Field: &schema.Field{Name: "group_a"}}, kind: aggregateValueString},
			{name: "group_b", out: "gb", measure: schema.MeasureFieldAccessor{Name: "group_b", Field: &schema.Field{Name: "group_b"}}, isMeasure: true, kind: aggregateValueUnsigned},
		},
		metrics: []aggregateMetric{
			{op: aggregateMetricMax, out: "max_score", field: aggregateFieldRef{name: "score"}, rowCount: true},
			{op: aggregateMetricCountDistinct, out: "unique_tags", field: aggregateFieldRef{name: "tags"}},
		},
		having: aggregateHavingExpr{op: aggregateHavingGT, index: 1, value: valueFromSafeString("42")},
		havingArgs: []aggregateHavingExpr{
			{op: aggregateHavingIN, index: 1, values: []Value{valueFromSafeString("42")}},
			{op: aggregateHavingExists, index: 2},
		},
		havingValues: []Value{valueFromSafeString("42"), Value{num: 7, any: ValueKindUint}},
		hasHaving:    true,
		order:        []aggregateOrder{{index: 1, desc: true}, {index: 2}},
		orderUnique:  true,
		offset:       3,
		limit:        5,
	}
	q.Release()

	if len(q.groups) != 0 || len(q.metrics) != 0 || len(q.havingArgs) != 0 || len(q.havingValues) != 0 ||
		len(q.order) != 0 || q.hasHaving || q.orderUnique || q.offset != 0 || q.limit != 0 {
		t.Fatalf("released aggregate query retained logical state")
	}
	if q.having.op != 0 || q.having.index != 0 || q.having.value.Kind() != ValueKindNone || q.having.values != nil || q.having.args != nil {
		t.Fatalf("released aggregate query retained having state: %+v", q.having)
	}

	for i, entry := range q.groups[:cap(q.groups)] {
		if entry.name != "" ||
			entry.out != "" ||
			entry.ordinary.Name != "" ||
			entry.ordinary.Field != nil ||
			entry.measure.Name != "" ||
			entry.measure.Field != nil ||
			entry.isMeasure ||
			entry.kind != aggregateValueInvalid {
			t.Fatalf("group slice retained stale entry at %d: %+v", i, entry)
		}
	}
	for i, entry := range q.metrics[:cap(q.metrics)] {
		if entry.op != aggregateMetricCount ||
			entry.out != "" ||
			entry.field.name != "" ||
			entry.field.out != "" ||
			entry.rowCount {
			t.Fatalf("metric slice retained stale entry at %d: %+v", i, entry)
		}
	}
	for i, entry := range q.havingArgs[:cap(q.havingArgs)] {
		if entry.op != 0 || entry.index != 0 || entry.value.Kind() != ValueKindNone || entry.values != nil || entry.args != nil {
			t.Fatalf("having arg slice retained stale entry at %d: %+v", i, entry)
		}
	}
	for i, entry := range q.havingValues[:cap(q.havingValues)] {
		if entry.Kind() != ValueKindNone {
			t.Fatalf("having value slice retained stale entry at %d: %+v", i, entry)
		}
	}
	for i, entry := range q.order[:cap(q.order)] {
		if entry.index != 0 || entry.desc {
			t.Fatalf("order slice retained stale entry at %d: %+v", i, entry)
		}
	}
}

func TestAggregateQueryReleaseDropsOversizedPlannerSlices(t *testing.T) {
	q := &Query{
		groups:       make([]aggregateFieldRef, 0, aggregateFieldRefSliceMaxCap+1),
		metrics:      make([]aggregateMetric, 0, aggregateMetricSliceMaxCap+1),
		havingArgs:   make([]aggregateHavingExpr, 0, aggregateHavingArgSliceMaxCap+1),
		havingValues: make([]Value, 0, aggregateHavingValueSliceMaxCap+1),
		order:        make([]aggregateOrder, 0, aggregateOrderSliceMaxCap+1),
	}
	q.Release()

	if q.groups != nil || q.metrics != nil || q.havingArgs != nil || q.havingValues != nil || q.order != nil {
		t.Fatalf("released aggregate query retained oversized slices")
	}
}

func TestCompactAggregateRowsDetachesKeptRows(t *testing.T) {
	values := []Value{
		{num: 1, any: ValueKindUint},
		{num: 10, any: ValueKindUint},
		{num: 2, any: ValueKindUint},
		{num: 20, any: ValueKindUint},
		{num: 3, any: ValueKindUint},
		{num: 30, any: ValueKindUint},
	}
	result := compactAggregateRows(Result{
		Layout: []string{"group", "rows"},
		Rows:   []Row{values[2:4:4]},
	})

	values[2] = Value{num: 200, any: ValueKindUint}
	values[3] = Value{num: 2000, any: ValueKindUint}

	group, _ := result.Rows[0][0].Uint()
	rows, _ := result.Rows[0][1].Uint()
	if group != 2 || rows != 20 {
		t.Fatalf("compacted row=(%d,%d), want (2,20)", group, rows)
	}
}

func TestShouldCompactAggregateRows(t *testing.T) {
	tests := []struct {
		before int
		after  int
		want   bool
	}{
		{before: 100, after: 0, want: false},
		{before: 100, after: 100, want: false},
		{before: 100, after: 90, want: false},
		{before: 100, after: 50, want: false},
		{before: 100, after: 49, want: true},
		{before: 100, after: 1, want: true},
	}
	for i := range tests {
		got := shouldCompactAggregateRows(tests[i].before, tests[i].after)
		if got != tests[i].want {
			t.Fatalf("case %d: shouldCompactAggregateRows(%d,%d)=%v want %v", i, tests[i].before, tests[i].after, got, tests[i].want)
		}
	}
}

func TestAggregateHavingStorageSizeValidatesLeftBeforeINValues(t *testing.T) {
	_, values, err := aggregateHavingStorageSize(
		qx.IN(qx.REF("country"), []uint64{1, 2, 3, 4}),
		map[string]int{"rows": 0},
	)
	if err == nil || !strings.Contains(err.Error(), "left side supports only OUT references") {
		t.Fatalf("aggregateHavingStorageSize err=%v", err)
	}
	if values != 0 {
		t.Fatalf("invalid HAVING IN counted values=%d", values)
	}
}

func TestAggregateMetricStateSlicePoolClearsFullCapacity(t *testing.T) {
	states := aggregateMetricStateSlicePool.Get(2)
	states = append(states,
		aggregateMetricState{
			metric:   aggregateMetric{op: aggregateMetricAvg, out: "avg_score", field: aggregateFieldRef{name: "score"}},
			seen:     true,
			count:    8,
			intSum:   -7,
			uintSum:  11,
			floatSum: 13.5,
			best:     valueFromSafeString("best"),
		},
		aggregateMetricState{
			metric:  aggregateMetric{op: aggregateMetricMin, out: "min_age", field: aggregateFieldRef{name: "age"}},
			seen:    true,
			count:   3,
			uintSum: 21,
			best:    valueFromSafeString("low"),
		},
	)
	aggregateMetricStateSlicePool.Put(states)

	for i, entry := range states[:cap(states)] {
		if entry.metric.op != aggregateMetricCount ||
			entry.metric.out != "" ||
			entry.metric.field.name != "" ||
			entry.seen ||
			entry.count != 0 ||
			entry.intSum != 0 ||
			entry.uintSum != 0 ||
			entry.floatSum != 0 ||
			entry.best.Kind() != ValueKindNone {
			t.Fatalf("metric state slice retained stale entry at %d: %+v", i, entry)
		}
	}
}

func TestAggregateValueFromIndexKeyCopiesBorrowedStringBytes(t *testing.T) {
	raw := []byte("group-alpha")
	key := keycodec.FromBytes(raw)
	field := schema.Field{Kind: reflect.String}

	value := aggregateValueFromIndexKey(&field, key)
	for i := range raw {
		raw[i] = 'x'
	}
	if key.UnsafeString() != "xxxxxxxxxxx" {
		t.Fatalf("test setup did not poison borrowed key: %q", key.UnsafeString())
	}

	got, ok := value.String()
	if !ok || got != "group-alpha" {
		t.Fatalf("aggregate value string=%q ok=%v want group-alpha/true", got, ok)
	}
}
