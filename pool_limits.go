package rbi

const (
	intSlicePoolMaxCap                 = 4 << 10
	uint64SlicePoolMaxCap              = 4 << 10
	roaringSlicePoolMaxCap             = 2 << 10
	postingListSlicePoolMaxCap         = 4 << 10
	bitmapResultSlicePoolMaxCap        = 2 << 10
	countORBranchSlicePoolMaxCap       = 512
	plannerOROrderIterSlicePoolMaxCap  = 512
	plannerOROrderMergeItemSliceMaxCap = 512
)
