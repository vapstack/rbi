package qexec

import (
	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const (
	stringSetPoolMaxLen = 4 << 10
	u64SetPoolMaxCap    = 16 << 10
)

var orderBasicBaseCoreSlicePool = pooled.Slices[orderBasicBaseCore]{MaxCap: 128, Clear: pooled.ClearCap}

/**/

var orderedMergedScalarRangeFieldSlicePool = pooled.Slices[orderedMergedScalarRangeField]{
	MaxCap: 64,
	Clear:  pooled.ClearCap,
}

/**/

var stringSetPool = pooled.Maps[string, struct{}]{
	NewCap: 64,
	MaxLen: stringSetPoolMaxLen,
}

/**/

var postingResultSlicePool = pooled.Slices[postingResult]{MaxCap: 2 << 10, Clear: pooled.ClearCap}

/**/

var cardinalityORBranchSlicePool = pooled.Slices[cardinalityORBranch]{
	MaxCap: 512,
	Clear:  pooled.ClearCap,
	Cleanup: func(buf []cardinalityORBranch) {
		for i := 0; i < len(buf); i++ {
			br := buf[i]
			br.preds.Release()
		}
	},
}

/**/

var cardinalityLeadResidualExactFilterSlicePool = pooled.Slices[cardinalityLeadResidualExactFilter]{
	MaxCap: 512,
	Clear:  pooled.ClearCap,
	Cleanup: func(buf []cardinalityLeadResidualExactFilter) {
		for i := 0; i < len(buf); i++ {
			buf[i].ids.Release()
		}
	},
}

/**/

var predicateSlicePool = pooled.Slices[predicate]{
	MaxCap: 256,
	Clear:  pooled.ClearCap,
	Cleanup: func(buf []predicate) {
		for i := 0; i < len(buf); i++ {
			releasePredicateOwnedState(&buf[i])
		}
	},
}

/**/

var leafPredSlicePool = pooled.Slices[leafPred]{
	MaxCap: 256,
	Clear:  pooled.ClearCap,
	Cleanup: func(buf []leafPred) {
		for i := 0; i < len(buf); i++ {
			pred := buf[i]
			if pred.kind == leafPredKindPredicate {
				releasePredicateOwnedState(&pred.pred)
			}
			if pred.postsAnyState != nil {
				postsAnyFilterStatePool.Put(pred.postsAnyState)
			}
			if pred.postsBuf != nil {
				posting.ReleaseSlice(pred.postsBuf)
			}
			if pred.hashAt < 0 {
				releaseU64Set(&pred.hash)
			}
		}
	},
}

var plannerORBranchSlicePool = pooled.Slices[plannerORBranch]{
	MaxCap: plannerORBranchLimit,
	Clear:  pooled.ClearCap,
	Cleanup: func(buf []plannerORBranch) {
		for i := 0; i < len(buf); i++ {
			preds := buf[i].preds
			preds.Release()
		}
	},
}

var plannerORPredicateBuildInfoSlicePool = pooled.Slices[orderedORMaterializedPredicateBuildInfo]{
	MaxCap: 64,
	Clear:  pooled.ClearCap,
}

var plannerOROrderIterSlicePool = pooled.Slices[plannerOROrderBranchIter]{
	MaxCap: 512,
	Clear:  pooled.ClearCap,
	Cleanup: func(buf []plannerOROrderBranchIter) {
		for i := 0; i < len(buf); i++ {
			iter := buf[i]
			iter.close()
		}
	},
}

/**/

var plannerOROrderMergeItemSlicePool = pooled.Slices[plannerOROrderMergeItem]{
	MaxCap: 512,
	Clear:  pooled.ClearCap,
}
