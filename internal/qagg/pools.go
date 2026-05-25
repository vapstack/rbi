package qagg

import "github.com/vapstack/rbi/internal/pooled"

const (
	aggregateMetricStateSlicePoolMaxCap = 1 << 20
	aggregateOrderSlicePoolMaxCap       = 4 << 10
	aggregateOutputPositionMapMaxLen    = 4 << 10
	aggregateGroupOrdinalMapMaxLen      = 8 << 20
)

var aggregateMetricStateSlicePool = pooled.NewSlicePool[aggregateMetricState](
	aggregateMetricStateSlicePoolMaxCap,
	pooled.ClearCap,
)

var aggregateOrderSlicePool = pooled.NewSlicePool[aggregateOrder](
	aggregateOrderSlicePoolMaxCap,
	pooled.ClearCap,
)

var aggregateOutputPositionMapPool = pooled.Maps[string, int]{
	NewCap: 8,
	MaxLen: aggregateOutputPositionMapMaxLen,
}

var aggregateGroupOrdinalMapPool = pooled.Pointers[aggregateGroupOrdinalMap]{
	Cleanup: func(m *aggregateGroupOrdinalMap) {
		m.reset()
	},
}
