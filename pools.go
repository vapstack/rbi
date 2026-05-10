package rbi

import (
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
)

const (
	materializedPredKeySlicePoolMaxCap      = 512
	stringSetPoolMaxLen                     = 4 << 10
	uint64IntMapPoolMaxLen                  = 16 << 10
	u64SetPoolMaxCap                        = 16 << 10
	exprSlicePoolMaxCap                     = 256
	bitmapResultSlicePoolMaxCap             = 2 << 10
	countORBranchSlicePoolMaxCap            = 512
	countLeadResidualExactFilterPoolMaxCap  = 512
	predicateSlicePoolMaxCap                = 256
	leafPredSlicePoolMaxCap                 = 256
	plannerORBranchSlicePoolMaxCap          = plannerORBranchLimit
	plannerORPredicateBuildInfoSliceMaxCap  = 64
	plannerOROrderIterSlicePoolMaxCap       = 512
	plannerOROrderMergeItemSliceMaxCap      = 512
	orderBasicBaseCoreSlicePoolMaxCap       = 128
	orderedMergedScalarRangeFieldPoolMaxCap = 64
	aggregateMetricStateSlicePoolMaxCap     = 1 << 20
)

/**/

var exprSlicePool = pooled.Slices[qir.Expr]{
	MinCap: 8,
	MaxCap: exprSlicePoolMaxCap,
	Clear:  true,
}

/**/

var orderBasicBaseCoreSlicePool = pooled.Slices[orderBasicBaseCore]{
	MinCap: 4,
	MaxCap: orderBasicBaseCoreSlicePoolMaxCap,
	Clear:  true,
}

/**/

var orderedMergedScalarRangeFieldSlicePool = pooled.Slices[orderedMergedScalarRangeField]{
	MinCap: 4,
	MaxCap: orderedMergedScalarRangeFieldPoolMaxCap,
	Clear:  true,
}

/**/

var aggregateMetricStateSlicePool = pooled.Slices[aggregateMetricState]{
	MinCap: 16,
	MaxCap: aggregateMetricStateSlicePoolMaxCap,
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

var postingResultSlicePool = pooled.Slices[postingResult]{
	MinCap: 16,
	MaxCap: bitmapResultSlicePoolMaxCap,
	Clear:  true,
}

/**/

var countORBranchSlicePool = pooled.Slices[countORBranch]{
	MinCap: 8,
	MaxCap: countORBranchSlicePoolMaxCap,
	Cleanup: func(buf *pooled.Slice[countORBranch]) {
		for i := 0; i < buf.Len(); i++ {
			br := buf.Get(i)
			releaseCountORBranchPredicates(br)
		}
	},
	Clear: true,
}

/**/

var countLeadResidualExactFilterSlicePool = pooled.Slices[countLeadResidualExactFilter]{
	MinCap: 8,
	MaxCap: countLeadResidualExactFilterPoolMaxCap,
	Cleanup: func(buf *pooled.Slice[countLeadResidualExactFilter]) {
		for i := 0; i < buf.Len(); i++ {
			buf.Get(i).ids.Release()
		}
	},
	Clear: true,
}

/**/

var predicateSlicePool = pooled.Slices[predicate]{
	MinCap: 8,
	MaxCap: predicateSlicePoolMaxCap,
	Cleanup: func(buf *pooled.Slice[predicate]) {
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
	Cleanup: func(buf *pooled.Slice[leafPred]) {
		for i := 0; i < buf.Len(); i++ {
			pred := buf.Get(i)
			if pred.kind == leafPredKindPredicate {
				releasePredicateOwnedState(&pred.pred)
			}
			if pred.postsAnyState != nil {
				postsAnyFilterStatePool.Put(pred.postsAnyState)
			}
			if pred.postsBuf != nil {
				posting.PutSlice(pred.postsBuf)
			}
		}
	},
	Clear: true,
}

var plannerORBranchSlicePool = pooled.Slices[plannerORBranch]{
	MinCap: 8,
	MaxCap: plannerORBranchSlicePoolMaxCap,
	Cleanup: func(buf *pooled.Slice[plannerORBranch]) {
		for i := 0; i < buf.Len(); i++ {
			releasePlannerORBranchPredicates(buf.Get(i))
		}
	},
	Clear: true,
}

var plannerORPredicateBuildInfoSlicePool = pooled.Slices[orderedORMaterializedPredicateBuildInfo]{
	MinCap: 8,
	MaxCap: plannerORPredicateBuildInfoSliceMaxCap,
	Clear:  true,
}

var plannerOROrderIterSlicePool = pooled.Slices[plannerOROrderBranchIter]{
	MinCap: 16,
	MaxCap: plannerOROrderIterSlicePoolMaxCap,
	Cleanup: func(buf *pooled.Slice[plannerOROrderBranchIter]) {
		for i := 0; i < buf.Len(); i++ {
			iter := buf.Get(i)
			iter.close()
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
