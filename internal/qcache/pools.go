package qcache

import "github.com/vapstack/rbi/internal/pooled"

const (
	materializedPredKeySlicePoolMaxCap     = 512
	recentKeyCacheSlotPoolMaxCap           = 512
	materializedPredCacheRetiredPoolMaxCap = 512
	materializedPredCacheIndexPoolMaxLen   = 512

	recentKeyCacheIndexPoolMaxLen = recentKeyCacheSlotPoolMaxCap + materializedPredCacheOversizedMaxEntries

	numericRangeBucketFieldIndexPoolMaxLen = 512
)

var materializedPredKeySlicePool = pooled.NewSlicePool[MaterializedPredKey](
	materializedPredKeySlicePoolMaxCap,
	pooled.ClearCap,
)

var recentKeyCacheSlotPool = pooled.NewSlicePool[recentKeyCacheSlot](
	recentKeyCacheSlotPoolMaxCap,
	pooled.ClearCap,
)

var materializedPredCacheIndexPool = pooled.Maps[uint64, int]{
	MaxLen: materializedPredCacheIndexPoolMaxLen,
}

var recentKeyCacheIndexPool = pooled.Maps[uint64, int]{
	MaxLen: recentKeyCacheIndexPoolMaxLen,
}

var materializedPredCachePool = pooled.Pointers[MaterializedPredCache]{
	Cleanup: func(c *MaterializedPredCache) {
		c.release()
	},
}

var materializedPredCacheRetiredPool = pooled.NewSlicePool[*materializedPredCacheEntry](
	materializedPredCacheRetiredPoolMaxCap,
	pooled.ClearCap,
)

var materializedPredCacheEntryPool = pooled.Pointers[materializedPredCacheEntry]{
	Clear: true,
}

var numericRangeBucketCachePool = pooled.Pointers[NumericRangeBucketCache]{
	Cleanup: func(c *NumericRangeBucketCache) {
		c.release()
	},
}

var numericRangeBucketFieldIndexPool = pooled.Maps[string, *NumericRangeBucketEntry]{
	MaxLen: numericRangeBucketFieldIndexPoolMaxLen,
}

var numericRangeBucketEntryPool = pooled.Pointers[NumericRangeBucketEntry]{
	Cleanup: func(e *NumericRangeBucketEntry) {
		e.releaseFullSpanCache()
	},
	Clear: true,
}
