package rbi

import (
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const (
	uint64SlicePoolMaxCap                   = 4 << 10
	stringSlicePoolMaxCap                   = 64 << 10
	materializedPredKeySlicePoolMaxCap      = 512
	stringSetPoolMaxLen                     = 4 << 10
	uint64IntMapPoolMaxLen                  = 16 << 10
	u64SetPoolMaxCap                        = 16 << 10
	exprSlicePoolMaxCap                     = 256
	boolSlicePoolMaxCap                     = 256
	postingSlicePoolMaxCap                  = 4 << 10
	postingMapPoolMaxLen                    = 4 << 10
	batchPostingDeltaMapPoolMaxLen          = 4 << 10
	keyedBatchPostingDeltaSliceMaxCap       = 4 << 10
	bitmapResultSlicePoolMaxCap             = 2 << 10
	countORBranchSlicePoolMaxCap            = 512
	countLeadResidualExactFilterPoolMaxCap  = 512
	predicateCheckSlicePoolMaxCap           = 4 << 10
	predicateSlicePoolMaxCap                = 256
	leafPredSlicePoolMaxCap                 = 256
	plannerOROrderIterSlicePoolMaxCap       = 512
	plannerOROrderMergeItemSliceMaxCap      = 512
	orderBasicBaseCoreSlicePoolMaxCap       = 128
	orderBasicBaseCoreIndexSlicePoolMaxCap  = 128
	orderedMergedScalarRangeFieldPoolMaxCap = 64
)

/**/

var exprSlicePool = pooled.Slices[qx.Expr]{
	MinCap: 8,
	MaxCap: exprSlicePoolMaxCap,
	Clear:  true,
}

/**/

var boolSlicePool = pooled.Slices[bool]{
	MinCap: 8,
	MaxCap: boolSlicePoolMaxCap,
	Clear:  true,
}

/**/

var orderBasicBaseCoreSlicePool = pooled.Slices[orderBasicBaseCore]{
	MinCap: 4,
	MaxCap: orderBasicBaseCoreSlicePoolMaxCap,
	Clear:  true,
}

/**/

var orderBasicBaseCoreIndexSlicePool = pooled.Slices[int]{
	MinCap: 8,
	MaxCap: orderBasicBaseCoreIndexSlicePoolMaxCap,
}

/**/

var predicateCheckSlicePool = pooled.Slices[int]{
	MinCap: 8,
	MaxCap: predicateCheckSlicePoolMaxCap,
}

/**/

var orderedMergedScalarRangeFieldSlicePool = pooled.Slices[orderedMergedScalarRangeField]{
	MinCap: 4,
	MaxCap: orderedMergedScalarRangeFieldPoolMaxCap,
	Clear:  true,
}

/**/

var uint64SlicePool = pooled.Slices[uint64]{
	MinCap: 64,
	MaxCap: uint64SlicePoolMaxCap,
}

/**/

var stringSlicePool = pooled.Slices[string]{
	MinCap: 64,
	MaxCap: stringSlicePoolMaxCap,
	Clear:  true,
}

/**/

var materializedPredKeySlicePool = pooled.Slices[materializedPredKey]{
	MinCap: 8,
	MaxCap: materializedPredKeySlicePoolMaxCap,
	Clear:  true,
}

/**/

var stringSetPool = pooled.Maps[string, struct{}]{
	NewCap: 8,
	MaxLen: stringSetPoolMaxLen,
	Cleanup: func(m map[string]struct{}) {
		clear(m)
	},
}

/**/

var uint64IntMapPool = pooled.Maps[uint64, int]{
	NewCap: 8,
	MaxLen: uint64IntMapPoolMaxLen,
	Cleanup: func(m map[uint64]int) {
		clear(m)
	},
}

/**/

var postingSlicePool = pooled.Slices[posting.List]{
	MinCap: 16,
	MaxCap: postingSlicePoolMaxCap,
	Clear:  true,
}

var postingMapPool = pooled.Maps[string, posting.List]{
	NewCap: 8,
	MaxLen: postingMapPoolMaxLen,
	Cleanup: func(m map[string]posting.List) {
		clear(m)
	},
}
var fixedPostingMapPool = pooled.Maps[uint64, posting.List]{
	NewCap: 8,
	MaxLen: postingMapPoolMaxLen,
	Cleanup: func(m map[uint64]posting.List) {
		clear(m)
	},
}

var keyedBatchPostingDeltaSlicePool = pooled.Slices[keyedBatchPostingDelta]{
	MinCap: 16,
	MaxCap: keyedBatchPostingDeltaSliceMaxCap,
	Cleanup: func(buf *pooled.SliceBuf[keyedBatchPostingDelta]) {
		for i := 0; i < buf.Len(); i++ {
			delta := buf.Get(i)
			delta.delta.add.Release()
			delta.delta.remove.Release()
			buf.Set(i, keyedBatchPostingDelta{})
		}
	},
	Clear: true,
}

var fixedBatchPostingDeltaMapPool = pooled.Maps[uint64, batchPostingDelta]{
	NewCap: 8,
	MaxLen: batchPostingDeltaMapPoolMaxLen,
}

var postingResultSlicePool = pooled.Slices[postingResult]{
	MinCap: 16,
	MaxCap: bitmapResultSlicePoolMaxCap,
	Clear:  true,
}

/**/

var countORBranchSlicePool = pooled.Slices[countORBranch]{
	MinCap: 8,
	MaxCap: countORBranchSlicePoolMaxCap,
	Cleanup: func(buf *pooled.SliceBuf[countORBranch]) {
		for i := 0; i < buf.Len(); i++ {
			br := buf.Get(i)
			releaseCountORBranchPredicates(br)
			buf.Set(i, countORBranch{})
		}
	},
	Clear: true,
}

/**/

var countLeadResidualExactFilterSlicePool = pooled.Slices[countLeadResidualExactFilter]{
	MinCap: 8,
	MaxCap: countLeadResidualExactFilterPoolMaxCap,
	Cleanup: func(buf *pooled.SliceBuf[countLeadResidualExactFilter]) {
		for i := 0; i < buf.Len(); i++ {
			filter := buf.Get(i)
			filter.ids.Release()
			filter.ids = posting.List{}
			filter.state = nil
			buf.Set(i, filter)
		}
	},
	Clear: true,
}

/**/

var predicateSlicePool = pooled.Slices[predicate]{
	MinCap: 8,
	MaxCap: predicateSlicePoolMaxCap,
	Cleanup: func(buf *pooled.SliceBuf[predicate]) {
		for i := 0; i < buf.Len(); i++ {
			releasePredicateOwnedState(buf.GetPtr(i))
		}
	},
	Clear: true,
}

/**/

var leafPredSlicePool = pooled.Slices[leafPred]{
	MinCap: 8,
	MaxCap: leafPredSlicePoolMaxCap,
	Cleanup: func(buf *pooled.SliceBuf[leafPred]) {
		for i := 0; i < buf.Len(); i++ {
			pred := buf.Get(i)
			if pred.kind == leafPredKindPredicate {
				releasePredicateOwnedState(&pred.pred)
			}
			if pred.postsAnyState != nil {
				postsAnyFilterStatePool.Put(pred.postsAnyState)
			}
			if pred.postsBuf != nil {
				postingSlicePool.Put(pred.postsBuf)
			}
			buf.Set(i, leafPred{})
		}
	},
	Clear: true,
}

var plannerOROrderIterSlicePool = pooled.Slices[plannerOROrderBranchIter]{
	MinCap: 16,
	MaxCap: plannerOROrderIterSlicePoolMaxCap,
	Cleanup: func(buf *pooled.SliceBuf[plannerOROrderBranchIter]) {
		for i := 0; i < buf.Len(); i++ {
			iter := buf.Get(i)
			iter.close()
			buf.Set(i, plannerOROrderBranchIter{})
		}
	},
	Clear: true,
}

/**/

var plannerOROrderMergeItemSlicePool = pooled.Slices[plannerOROrderMergeItem]{
	MinCap: 16,
	MaxCap: plannerOROrderMergeItemSliceMaxCap,
	Clear:  true,
}

/**/

var (
	uniqueLeavingOuterPool = pooled.Maps[string, map[string]posting.List]{
		NewCap: 8,
		MaxLen: pooledUniqueOuterMaxLen,
		Cleanup: func(m map[string]map[string]posting.List) {
			for _, inner := range m {
				uniqueLeavingInnerPool.Put(inner)
			}
			clear(m)
		},
	}
	uniqueLeavingInnerPool = pooled.Maps[string, posting.List]{
		NewCap: 8,
		MaxLen: pooledUniqueInnerMaxLen,
		Cleanup: func(m map[string]posting.List) {
			for _, ids := range m {
				ids.Release()
			}
			clear(m)
		},
	}
	uniqueSeenOuterPool = pooled.Maps[string, map[string]uint64]{
		NewCap: 8,
		MaxLen: pooledUniqueOuterMaxLen,
		Cleanup: func(m map[string]map[string]uint64) {
			for _, inner := range m {
				uniqueSeenInnerPool.Put(inner)
			}
			clear(m)
		},
	}
	uniqueSeenInnerPool = pooled.Maps[string, uint64]{
		NewCap: 8,
		MaxLen: pooledUniqueInnerMaxLen,
		Cleanup: func(m map[string]uint64) {
			clear(m)
		},
	}
)
