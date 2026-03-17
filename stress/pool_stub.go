//go:build !rbidebug

package main

import "github.com/vapstack/rbi"

type poolStats struct{}

type poolSample struct {
	CapturedAt string    `json:"captured_at"`
	Stats      poolStats `json:"stats"`
	Delta      poolDelta `json:"delta"`
}

type poolSummary struct {
	FinalDelta poolDelta `json:"final_delta"`
}

type poolDelta struct {
	Roaring64 roaringPoolDelta `json:"roaring64"`
	Scratch   scratchPoolDelta `json:"scratch"`
	Writes    writePoolDelta   `json:"writes"`
}

type roaringPoolDelta struct {
	BitmapContainers      poolCounterDelta             `json:"bitmap_containers"`
	ArrayContainers       poolCounterDelta             `json:"array_containers"`
	ArrayContainerClasses arrayContainerClassPoolDelta `json:"array_container_classes"`
	BitmapIterators       poolCounterDelta             `json:"bitmap_iterators"`
	ManyBitmapIterators   poolCounterDelta             `json:"many_bitmap_iterators"`
	BitmapIterators64     poolCounterDelta             `json:"bitmap_iterators64"`
	AddManyBatches64      slicePoolDelta               `json:"add_many_batches64"`
}

type scratchPoolDelta struct {
	RoaringBitmap                poolCounterDelta `json:"roaring_bitmap"`
	IntSlice                     slicePoolDelta   `json:"int_slice"`
	Uint64Slice                  slicePoolDelta   `json:"uint64_slice"`
	RoaringSlice                 slicePoolDelta   `json:"roaring_slice"`
	PostingListSlice             slicePoolDelta   `json:"posting_list_slice"`
	BitmapResultSlice            slicePoolDelta   `json:"bitmap_result_slice"`
	CountORBranchSlice           slicePoolDelta   `json:"count_or_branch_slice"`
	PlannerOROrderIterSlice      slicePoolDelta   `json:"planner_or_order_iter_slice"`
	PlannerOROrderMergeItemSlice slicePoolDelta   `json:"planner_or_order_merge_item_slice"`
}

type writePoolDelta struct {
	UniqueLeavingOuter mapPoolDelta `json:"unique_leaving_outer"`
	UniqueLeavingInner mapPoolDelta `json:"unique_leaving_inner"`
	UniqueSeenOuter    mapPoolDelta `json:"unique_seen_outer"`
	UniqueSeenInner    mapPoolDelta `json:"unique_seen_inner"`
	WriteDeltaOuter    mapPoolDelta `json:"write_delta_outer"`
	WriteDeltaInner    mapPoolDelta `json:"write_delta_inner"`
}

type poolCounterDelta struct {
	Gets         uint64 `json:"gets"`
	Hits         uint64 `json:"hits"`
	Misses       uint64 `json:"misses"`
	Puts         uint64 `json:"puts"`
	DropNil      uint64 `json:"drop_nil"`
	DropRejected uint64 `json:"drop_rejected"`
}

type slicePoolDelta struct {
	poolCounterDelta
	MaxRequestedCapacity int `json:"max_requested_capacity"`
	MaxReturnedCapacity  int `json:"max_returned_capacity"`
}

type mapPoolDelta struct {
	poolCounterDelta
	MaxReturnedEntries int `json:"max_returned_entries"`
}

type arrayContainerClassPoolDelta struct {
	MaxPooledCapacity    int                        `json:"max_pooled_capacity"`
	MaxRequestedCapacity int                        `json:"max_requested_capacity"`
	MaxReturnedCapacity  int                        `json:"max_returned_capacity"`
	DirectAllocs         uint64                     `json:"direct_allocs"`
	DirectAllocElements  uint64                     `json:"direct_alloc_elements"`
	DropBelowMinCapacity uint64                     `json:"drop_below_min_capacity"`
	DropOutOfRange       uint64                     `json:"drop_out_of_range"`
	Classes              []arrayContainerClassDelta `json:"classes,omitempty"`
}

type arrayContainerClassDelta struct {
	Capacity int    `json:"capacity"`
	Gets     uint64 `json:"gets"`
	Hits     uint64 `json:"hits"`
	Misses   uint64 `json:"misses"`
	Puts     uint64 `json:"puts"`
}

func resetPoolStatsWindow(*rbi.DB[uint64, UserBench]) {}

func poolStatsSinceReset(*rbi.DB[uint64, UserBench]) poolStats {
	return poolStats{}
}

func makePoolSample(now string, _ poolStats, _ poolStats) poolSample {
	return poolSample{CapturedAt: now}
}

func summarizePools(samples []poolSample, final *poolSample) *poolSummary {
	if len(samples) == 0 && final == nil {
		return nil
	}
	if final == nil {
		last := samples[len(samples)-1]
		return &poolSummary{FinalDelta: last.Delta}
	}
	return &poolSummary{FinalDelta: final.Delta}
}

func poolReportData(*poolSample, *poolSample, []poolSample) (*poolSample, *poolSample, *poolSummary, []poolSample) {
	return nil, nil, nil, nil
}
