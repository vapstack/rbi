package qcache

import (
	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const (
	materializedPredKeySlicePoolMaxCap     = 512
	recentKeyCacheSlotPoolMaxCap           = 512
	materializedPredCacheRetiredPoolMaxCap = 512
	materializedPredCacheIndexPoolMaxLen   = 512
	numericRangeBucketRetiredPoolMaxCap    = 512

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

var materializedPredCacheRetiredPool = pooled.Slices[*materializedPredCacheEntry]{
	MaxCap: materializedPredCacheRetiredPoolMaxCap,
	Clear:  pooled.ClearCap,
}

var numericRangeBucketRetiredPool = pooled.Slices[[]posting.List]{
	MaxCap: numericRangeBucketRetiredPoolMaxCap,
	Clear:  pooled.ClearCap,
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
		e.releaseFullSpanCache()
	},
	Clear: true,
}
