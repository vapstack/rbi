package qexec

import (
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const (
	stringSetPoolMaxLen = 4 << 10
	u64SetPoolMaxCap    = 16 << 10
)

var orderBasicBaseCoreSlicePool = pooled.NewSlicePool[orderBasicBaseCore](128, pooled.ClearCap)

/**/

var orderedMergedScalarRangeFieldSlicePool = pooled.NewSlicePool[orderedMergedScalarRangeField](
	64,
	pooled.ClearCap,
)

/**/

var stringSetPool = pooled.Maps[string, struct{}]{
	NewCap: 8,
	MaxLen: stringSetPoolMaxLen,
	Cleanup: func(m map[string]struct{}) {
		clear(m)
	},
}

/**/

var postingResultSlicePool = pooled.NewSlicePool[postingResult](2<<10, pooled.ClearCap)

/**/

var cardinalityORBranchSlicePool = pooled.NewSlicePool[cardinalityORBranch](
	512,
	pooled.ClearCap,
	func(buf []cardinalityORBranch) {
		for i := 0; i < len(buf); i++ {
			br := buf[i]
			br.preds.Release()
		}
	},
)

/**/

var cardinalityLeadResidualExactFilterSlicePool = pooled.NewSlicePool[cardinalityLeadResidualExactFilter](
	512,
	pooled.ClearCap,
	func(buf []cardinalityLeadResidualExactFilter) {
		for i := 0; i < len(buf); i++ {
			buf[i].ids.Release()
		}
	},
)

/**/

var predicateSlicePool = pooled.NewSlicePool[predicate](256, pooled.ClearCap,
	func(buf []predicate) {
		for i := 0; i < len(buf); i++ {
			releasePredicateOwnedState(&buf[i])
		}
	},
)

/**/

var leafPredSlicePool = pooled.NewSlicePool[leafPred](256, pooled.ClearCap,
	func(buf []leafPred) {
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
		}
	},
)

var plannerORBranchSlicePool = pooled.NewSlicePool[plannerORBranch](plannerORBranchLimit, pooled.ClearCap,
	func(buf []plannerORBranch) {
		for i := 0; i < len(buf); i++ {
			preds := buf[i].preds
			preds.Release()
		}
	},
)

var plannerORPredicateBuildInfoSlicePool = pooled.NewSlicePool[orderedORMaterializedPredicateBuildInfo](
	64,
	pooled.ClearCap,
)

var plannerOROrderIterSlicePool = pooled.NewSlicePool[plannerOROrderBranchIter](
	512,
	pooled.ClearCap,
	func(buf []plannerOROrderBranchIter) {
		for i := 0; i < len(buf); i++ {
			iter := buf[i]
			iter.close()
		}
	},
)

/**/

var plannerOROrderMergeItemSlicePool = pooled.NewSlicePool[plannerOROrderMergeItem](
	512,
	pooled.ClearCap,
)
