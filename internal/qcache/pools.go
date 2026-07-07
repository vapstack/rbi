package qcache

import (
	"github.com/vapstack/pooled"
)

const (
	materializedPredKeySlicePoolMaxCap     = 512
	recentKeyCacheSlotPoolMaxCap           = 512
	materializedPredCacheRetiredPoolMaxCap = 512
	materializedPredCacheIndexPoolMaxLen   = 512
	numericRangeBucketRetiredPoolMaxCap    = 512
	numericRangeExactRetiredPoolMaxCap     = 512
	numericRangeSpanObservedSlotPoolMaxCap = 512
	numericRangeSpanCacheIndexPoolMaxLen   = 512

	recentKeyCacheIndexPoolMaxLen = recentKeyCacheSlotPoolMaxCap + materializedPredCacheOversizedMaxEntries

	numericRangeBucketFieldIndexPoolMaxLen = 512
)

var materializedPredKeySlicePool = pooled.Slices[MaterializedPredKey]{
	MaxCap: materializedPredKeySlicePoolMaxCap,
	Clear:  pooled.ClearCap,
}

var recentKeyCacheSlotPool = pooled.Slices[recentKeyCacheSlot]{
	MaxCap: recentKeyCacheSlotPoolMaxCap,
	Clear:  pooled.ClearCap,
}

var materializedPredCacheIndexPool = pooled.Maps[uint64, int]{
	NewCap: 128,
	MaxLen: materializedPredCacheIndexPoolMaxLen,
}

var recentKeyCacheIndexPool = pooled.Maps[uint64, int]{
	NewCap: 128,
	MaxLen: recentKeyCacheIndexPoolMaxLen,
}

var materializedPredCachePool = pooled.Pointers[MaterializedPredCache]{
	Cleanup: func(c *MaterializedPredCache) {
		c.release()
	},
}

var materializedPredCacheRetiredPool = pooled.Slices[materializedPredRetiredEntry]{
	MaxCap: materializedPredCacheRetiredPoolMaxCap,
	Clear:  pooled.ClearCap,
}

var numericRangeBucketRetiredPool = pooled.Slices[numericRangeSpanRetiredEntry]{
	MaxCap: numericRangeBucketRetiredPoolMaxCap,
	Clear:  pooled.ClearCap,
}

var numericRangeExactRetiredPool = pooled.Slices[numericRangeExactRetiredEntry]{
	MaxCap: numericRangeExactRetiredPoolMaxCap,
	Clear:  pooled.ClearCap,
}

var numericRangeSpanObservedSlotPool = pooled.Slices[numericRangeSpanObservedSlot]{
	MaxCap: numericRangeSpanObservedSlotPoolMaxCap,
	Clear:  pooled.ClearCap,
}

var numericRangeSpanCacheIndexPool = pooled.Maps[numericRangeSpanKey, int]{
	NewCap: 128,
	MaxLen: numericRangeSpanCacheIndexPoolMaxLen,
}

var numericRangeSpanObservedIndexPool = pooled.Maps[numericRangeSpanKey, int]{
	NewCap: 128,
	MaxLen: numericRangeSpanObservedSlotPoolMaxCap,
}

var numericRangeCachedPostingPool = pooled.Pointers[numericRangeCachedPosting]{
	Clear: true,
}

var materializedPredCacheEntryPool = pooled.Pointers[materializedPredCacheEntry]{
	Clear: true,
}

var numericRangeBucketCachePool = pooled.Pointers[NumericRangeBucketCache]{
	Cleanup: func(c *NumericRangeBucketCache) {
		c.release()
	},
}

var numericRangeBucketFieldIndexPool = pooled.Maps[string, *NumericRangeBucketEntry]{
	NewCap: 64,
	MaxLen: numericRangeBucketFieldIndexPoolMaxLen,
}

var numericRangeBucketEntryPool = pooled.Pointers[NumericRangeBucketEntry]{
	Cleanup: func(e *NumericRangeBucketEntry) {
		e.resetRecentFullSpans()
	},
	Clear: true,
}
