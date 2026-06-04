package qagg

import "github.com/vapstack/pooled"

const (
	aggregateMetricStateSlicePoolMaxCap = 1 << 20
	aggregateMetricSliceMaxCap          = 4 << 10
	aggregateFieldRefSliceMaxCap        = 4 << 10
	aggregateOrderSliceMaxCap           = 4 << 10
	aggregateOutputPositionMapMaxLen    = 4 << 10
	aggregateGroupOrdinalMapMaxLen      = 8 << 20
	aggregateGroupOrdinalMapPoolMaxCap  = 1 << 20
)

var aggregateQueryPool = pooled.Pointers[Query]{
	Cleanup: func(q *Query) {
		if q.filter != nil {
			q.filter.Release()
			q.filter = nil
		}
		if cap(q.groups) > aggregateFieldRefSliceMaxCap {
			q.groups = nil
		} else {
			clear(q.groups[:cap(q.groups)])
			q.groups = q.groups[:0]
		}
		if cap(q.metrics) > aggregateMetricSliceMaxCap {
			q.metrics = nil
		} else {
			clear(q.metrics[:cap(q.metrics)])
			q.metrics = q.metrics[:0]
		}
		q.having = aggregateHavingExpr{}
		q.hasHaving = false
		if cap(q.order) > aggregateOrderSliceMaxCap {
			q.order = nil
		} else {
			clear(q.order[:cap(q.order)])
			q.order = q.order[:0]
		}
		q.orderUnique = false
		q.offset = 0
		q.limit = 0
	},
}

var aggregateMetricStateSlicePool = pooled.Slices[aggregateMetricState]{
	MaxCap: aggregateMetricStateSlicePoolMaxCap,
	Clear:  pooled.ClearCap,
}

var aggregateOutputPositionMapPool = pooled.Maps[string, int]{
	NewCap: 32,
	MaxLen: aggregateOutputPositionMapMaxLen,
}

var aggregateGroupOrdinalMapPool = pooled.Pointers[aggregateGroupOrdinalMap]{
	Cleanup: func(m *aggregateGroupOrdinalMap) {
		m.reset()
	},
}
