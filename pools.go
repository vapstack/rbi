package rbi

import (
	"slices"
	"sync"

	"github.com/vapstack/rbi/internal/posting"
)

const (
	intSlicePoolMaxCap                 = 4 << 10
	uint64SlicePoolMaxCap              = 4 << 10
	stringSlicePoolMaxCap              = 64 << 10
	stringSetPoolMaxLen                = 4 << 10
	uint64IntMapPoolMaxLen             = 16 << 10
	u64SetPoolMaxCap                   = 16 << 10
	postingSlicePoolMaxCap             = 4 << 10
	postingMapPoolMaxLen               = 4 << 10
	batchPostingDeltaMapPoolMaxLen     = 4 << 10
	keyedBatchPostingDeltaSliceMaxCap  = 4 << 10
	bitmapResultSlicePoolMaxCap        = 2 << 10
	countORBranchSlicePoolMaxCap       = 512
	plannerOROrderIterSlicePoolMaxCap  = 512
	plannerOROrderMergeItemSliceMaxCap = 512
)

/**/

type intSliceBuf struct{ values []int }

var intSlicePool sync.Pool

func getIntSliceBuf(capHint int) *intSliceBuf {
	if v := intSlicePool.Get(); v != nil {
		buf := v.(*intSliceBuf)
		if cap(buf.values) < capHint {
			buf.values = slices.Grow(buf.values, capHint)
		}
		return buf
	}
	return &intSliceBuf{values: make([]int, 0, max(capHint, 64))}
}

func releaseIntSliceBuf(buf *intSliceBuf) {
	if buf == nil {
		return
	}
	if cap(buf.values) > intSlicePoolMaxCap {
		return
	}
	buf.values = buf.values[:0]
	intSlicePool.Put(buf)
}

/**/

type uint64SliceBuf struct{ values []uint64 }

var uint64SlicePool sync.Pool

func getUint64SliceBuf(capHint int) *uint64SliceBuf {
	if v := uint64SlicePool.Get(); v != nil {
		buf := v.(*uint64SliceBuf)
		if cap(buf.values) < capHint {
			buf.values = slices.Grow(buf.values, capHint)
		}
		return buf
	}
	return &uint64SliceBuf{values: make([]uint64, 0, max(capHint, 64))}
}

func releaseUint64SliceBuf(buf *uint64SliceBuf) {
	if buf == nil {
		return
	}
	if cap(buf.values) > uint64SlicePoolMaxCap {
		return
	}
	buf.values = buf.values[:0]
	uint64SlicePool.Put(buf)
}

/**/

type stringSliceBuf struct{ values []string }

var stringSlicePool sync.Pool

func getStringSliceBuf(capHint int) *stringSliceBuf {
	if v := stringSlicePool.Get(); v != nil {
		buf := v.(*stringSliceBuf)
		if cap(buf.values) < capHint {
			buf.values = slices.Grow(buf.values, capHint)
		}
		return buf
	}
	return &stringSliceBuf{values: make([]string, 0, max(capHint, 64))}
}

func releaseStringSliceBuf(buf *stringSliceBuf) {
	if buf == nil {
		return
	}
	if cap(buf.values) > stringSlicePoolMaxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	stringSlicePool.Put(buf)
}

/**/

var stringSetPool sync.Pool

func getStringSet(capHint int) map[string]struct{} {
	if v := stringSetPool.Get(); v != nil {
		return v.(map[string]struct{})
	}
	return make(map[string]struct{}, max(capHint, 8))
}

func releaseStringSet(m map[string]struct{}) {
	if m == nil {
		return
	}
	oversized := len(m) > stringSetPoolMaxLen
	clear(m)
	if oversized {
		return
	}
	stringSetPool.Put(m)
}

/**/

var uint64IntMapPool sync.Pool

func getUint64IntMap(capHint int) map[uint64]int {
	if v := uint64IntMapPool.Get(); v != nil {
		return v.(map[uint64]int)
	}
	return make(map[uint64]int, max(capHint, 8))
}

func releaseUint64IntMap(m map[uint64]int) {
	if len(m) > uint64IntMapPoolMaxLen {
		return
	}
	clear(m)
	uint64IntMapPool.Put(m)
}

/**/

type postingSliceBuf struct{ values []posting.List }

var postingSlicePool sync.Pool
var postingMapPool sync.Pool
var fixedPostingMapPool sync.Pool

func getPostingSliceBuf(capHint int) *postingSliceBuf {
	if v := postingSlicePool.Get(); v != nil {
		buf := v.(*postingSliceBuf)
		if cap(buf.values) < capHint {
			buf.values = slices.Grow(buf.values, capHint)
		}
		return buf
	}
	return &postingSliceBuf{values: make([]posting.List, 0, max(capHint, 16))}
}

func releasePostingSliceBuf(buf *postingSliceBuf) {
	if buf == nil {
		return
	}
	if cap(buf.values) > postingSlicePoolMaxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	postingSlicePool.Put(buf)
}

func getPostingMap() map[string]posting.List {
	if v := postingMapPool.Get(); v != nil {
		return v.(map[string]posting.List)
	}
	return make(map[string]posting.List, 8)
}

func getFixedPostingMap() map[uint64]posting.List {
	if v := fixedPostingMapPool.Get(); v != nil {
		return v.(map[uint64]posting.List)
	}
	return make(map[uint64]posting.List, 8)
}

func releasePostingMap(m map[string]posting.List) {
	if m == nil {
		return
	}
	oversized := len(m) > postingMapPoolMaxLen
	clear(m)
	if oversized {
		return
	}
	postingMapPool.Put(m)
}

func releaseFixedPostingMap(m map[uint64]posting.List) {
	if m == nil {
		return
	}
	oversized := len(m) > postingMapPoolMaxLen
	clear(m)
	if oversized {
		return
	}
	fixedPostingMapPool.Put(m)
}

func releasePostingMapOwned(m map[string]posting.List) {
	if m == nil {
		return
	}
	oversized := len(m) > postingMapPoolMaxLen
	for _, ids := range m {
		ids.Release()
	}
	clear(m)
	if oversized {
		return
	}
	postingMapPool.Put(m)
}

func releaseFixedPostingMapOwned(m map[uint64]posting.List) {
	if m == nil {
		return
	}
	oversized := len(m) > postingMapPoolMaxLen
	for _, ids := range m {
		ids.Release()
	}
	clear(m)
	if oversized {
		return
	}
	fixedPostingMapPool.Put(m)
}

/**/

type keyedBatchPostingDeltaSliceBuf struct{ values []keyedBatchPostingDelta }

var keyedBatchPostingDeltaSlicePool sync.Pool
var batchPostingDeltaMapPool sync.Pool
var fixedBatchPostingDeltaMapPool sync.Pool

func getKeyedBatchPostingDeltaSliceBuf(capHint int) *keyedBatchPostingDeltaSliceBuf {
	if v := keyedBatchPostingDeltaSlicePool.Get(); v != nil {
		buf := v.(*keyedBatchPostingDeltaSliceBuf)
		if cap(buf.values) < capHint {
			buf.values = slices.Grow(buf.values, capHint)
		}
		return buf
	}
	return &keyedBatchPostingDeltaSliceBuf{values: make([]keyedBatchPostingDelta, 0, max(capHint, 16))}
}

func releaseKeyedBatchPostingDeltaSliceBuf(buf *keyedBatchPostingDeltaSliceBuf) {
	if buf == nil {
		return
	}
	if cap(buf.values) > keyedBatchPostingDeltaSliceMaxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	keyedBatchPostingDeltaSlicePool.Put(buf)
}

func getBatchPostingDeltaMap() map[string]batchPostingDelta {
	if v := batchPostingDeltaMapPool.Get(); v != nil {
		return v.(map[string]batchPostingDelta)
	}
	return make(map[string]batchPostingDelta, 8)
}

func getFixedBatchPostingDeltaMap() map[uint64]batchPostingDelta {
	if v := fixedBatchPostingDeltaMapPool.Get(); v != nil {
		return v.(map[uint64]batchPostingDelta)
	}
	return make(map[uint64]batchPostingDelta, 8)
}

func releaseBatchPostingDeltaMap(m map[string]batchPostingDelta) {
	if m == nil {
		return
	}
	oversized := len(m) > batchPostingDeltaMapPoolMaxLen
	clear(m)
	if oversized {
		return
	}
	batchPostingDeltaMapPool.Put(m)
}

func releaseFixedBatchPostingDeltaMap(m map[uint64]batchPostingDelta) {
	if m == nil {
		return
	}
	oversized := len(m) > batchPostingDeltaMapPoolMaxLen
	clear(m)
	if oversized {
		return
	}
	fixedBatchPostingDeltaMapPool.Put(m)
}

/**/

type postingResultSliceBuf struct{ values []postingResult }

var bitmapResultSlicePool sync.Pool

func getPostingResultSliceBuf(capHint int) *postingResultSliceBuf {
	if v := bitmapResultSlicePool.Get(); v != nil {
		buf := v.(*postingResultSliceBuf)
		if cap(buf.values) < capHint {
			buf.values = slices.Grow(buf.values, capHint)
		}
		return buf
	}
	return &postingResultSliceBuf{values: make([]postingResult, 0, max(capHint, 16))}
}

func releasePostingResultSliceBuf(buf *postingResultSliceBuf) {
	if cap(buf.values) > bitmapResultSlicePoolMaxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	bitmapResultSlicePool.Put(buf)
}

/**/

type countORBranchSliceBuf struct{ values countORBranches }

var countORBranchSlicePool sync.Pool

func getCountORBranchSliceBuf(capHint int) *countORBranchSliceBuf {
	if v := countORBranchSlicePool.Get(); v != nil {
		buf := v.(*countORBranchSliceBuf)
		if cap(buf.values) < capHint {
			buf.values = slices.Grow(buf.values, capHint)
		}
		return buf
	}
	return &countORBranchSliceBuf{values: make(countORBranches, 0, max(capHint, 8))}
}

func releaseCountORBranchSliceBuf(buf *countORBranchSliceBuf) {
	if buf == nil {
		return
	}
	if cap(buf.values) > countORBranchSlicePoolMaxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	countORBranchSlicePool.Put(buf)
}

/**/

type plannerOROrderIterSliceBuf struct{ values []plannerOROrderBranchIter }

var plannerOROrderIterSlicePool sync.Pool

func getPlannerOROrderIterSliceBuf(capHint int) *plannerOROrderIterSliceBuf {
	if v := plannerOROrderIterSlicePool.Get(); v != nil {
		buf := v.(*plannerOROrderIterSliceBuf)
		if cap(buf.values) < capHint {
			buf.values = slices.Grow(buf.values, capHint)
		}
		return buf
	}
	return &plannerOROrderIterSliceBuf{values: make([]plannerOROrderBranchIter, 0, max(capHint, 16))}
}

func releasePlannerOROrderIterSliceBuf(buf *plannerOROrderIterSliceBuf) {
	if buf == nil {
		return
	}
	if cap(buf.values) > plannerOROrderIterSlicePoolMaxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	plannerOROrderIterSlicePool.Put(buf)
}

/**/

type plannerOROrderMergeItemSliceBuf struct{ values []plannerOROrderMergeItem }

var plannerOROrderMergeItemSlicePool sync.Pool

func getPlannerOROrderMergeItemSliceBuf(capHint int) *plannerOROrderMergeItemSliceBuf {
	if v := plannerOROrderMergeItemSlicePool.Get(); v != nil {
		buf := v.(*plannerOROrderMergeItemSliceBuf)
		if cap(buf.values) < capHint {
			buf.values = slices.Grow(buf.values, capHint)
		}
		return buf
	}
	return &plannerOROrderMergeItemSliceBuf{values: make([]plannerOROrderMergeItem, 0, max(capHint, 16))}
}

func releasePlannerOROrderMergeItemSliceBuf(buf *plannerOROrderMergeItemSliceBuf) {
	if buf == nil {
		return
	}
	if cap(buf.values) > plannerOROrderMergeItemSliceMaxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	plannerOROrderMergeItemSlicePool.Put(buf)
}

/**/

var (
	uniqueLeavingOuterPool sync.Pool
	uniqueLeavingInnerPool sync.Pool
	uniqueSeenOuterPool    sync.Pool
	uniqueSeenInnerPool    sync.Pool
)

func getUniqueLeavingOuterMap() map[string]map[string]posting.List {
	if v := uniqueLeavingOuterPool.Get(); v != nil {
		return v.(map[string]map[string]posting.List)
	}
	return make(map[string]map[string]posting.List, 8)
}

func releaseUniqueLeavingOuterMap(m map[string]map[string]posting.List) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueOuterMaxLen
	for _, inner := range m {
		releaseUniqueLeavingInnerMap(inner)
	}
	clear(m)
	if oversized {
		return
	}
	uniqueLeavingOuterPool.Put(m)
}

func getUniqueLeavingInnerMap() map[string]posting.List {
	if v := uniqueLeavingInnerPool.Get(); v != nil {
		return v.(map[string]posting.List)
	}
	return make(map[string]posting.List, 8)
}

func releaseUniqueLeavingInnerMap(m map[string]posting.List) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueInnerMaxLen
	for _, ids := range m {
		ids.Release()
	}
	clear(m)
	if oversized {
		return
	}
	uniqueLeavingInnerPool.Put(m)
}

func getUniqueSeenOuterMap() map[string]map[string]uint64 {
	if v := uniqueSeenOuterPool.Get(); v != nil {
		return v.(map[string]map[string]uint64)
	}
	return make(map[string]map[string]uint64, 8)
}

func releaseUniqueSeenOuterMap(m map[string]map[string]uint64) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueOuterMaxLen
	for _, inner := range m {
		releaseUniqueSeenInnerMap(inner)
	}
	clear(m)
	if oversized {
		return
	}
	uniqueSeenOuterPool.Put(m)
}

func getUniqueSeenInnerMap() map[string]uint64 {
	if v := uniqueSeenInnerPool.Get(); v != nil {
		return v.(map[string]uint64)
	}
	return make(map[string]uint64, 8)
}

func releaseUniqueSeenInnerMap(m map[string]uint64) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueInnerMaxLen
	clear(m)
	if oversized {
		return
	}
	uniqueSeenInnerPool.Put(m)
}
