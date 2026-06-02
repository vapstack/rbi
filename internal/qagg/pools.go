package qagg

import "github.com/vapstack/pooled"

const (
	aggregateMetricStateSlicePoolMaxCap = 1 << 20
	aggregateMetricSlicePoolMaxCap      = 4 << 10
	aggregateFieldRefSlicePoolMaxCap    = 4 << 10
	aggregateOrderSlicePoolMaxCap       = 4 << 10
	aggregateOutputPositionMapMaxLen    = 4 << 10
	aggregateGroupOrdinalMapMaxLen      = 8 << 20
	aggregateGroupOrdinalMapPoolMaxCap  = 1 << 20
)

var aggregateMetricStateSlicePool = pooled.Slices[aggregateMetricState]{
	MaxCap: aggregateMetricStateSlicePoolMaxCap,
	Clear:  pooled.ClearCap,
}

var aggregateMetricSlicePool = pooled.Slices[aggregateMetric]{
	MaxCap: aggregateMetricSlicePoolMaxCap,
	Clear:  pooled.ClearCap,
}

var aggregateFieldRefSlicePool = pooled.Slices[aggregateFieldRef]{
	MaxCap: aggregateFieldRefSlicePoolMaxCap,
	Clear:  pooled.ClearCap,
}

var aggregateOrderSlicePool = pooled.Slices[aggregateOrder]{
	MaxCap: aggregateOrderSlicePoolMaxCap,
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
