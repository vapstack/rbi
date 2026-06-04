package qagg

import (
	"reflect"
	"testing"

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
		having:      aggregateHavingExpr{op: aggregateHavingGT, index: 1, value: valueFromSafeString("42")},
		hasHaving:   true,
		order:       []aggregateOrder{{index: 1, desc: true}, {index: 2}},
		orderUnique: true,
		offset:      3,
		limit:       5,
	}
	q.Release()

	if len(q.groups) != 0 || len(q.metrics) != 0 || len(q.order) != 0 || q.hasHaving || q.orderUnique || q.offset != 0 || q.limit != 0 {
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
	for i, entry := range q.order[:cap(q.order)] {
		if entry.index != 0 || entry.desc {
			t.Fatalf("order slice retained stale entry at %d: %+v", i, entry)
		}
	}
}

func TestAggregateQueryReleaseDropsOversizedPlannerSlices(t *testing.T) {
	q := &Query{
		groups:  make([]aggregateFieldRef, 0, aggregateFieldRefSliceMaxCap+1),
		metrics: make([]aggregateMetric, 0, aggregateMetricSliceMaxCap+1),
		order:   make([]aggregateOrder, 0, aggregateOrderSliceMaxCap+1),
	}
	q.Release()

	if q.groups != nil || q.metrics != nil || q.order != nil {
		t.Fatalf("released aggregate query retained oversized slices")
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
