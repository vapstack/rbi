package main

import (
	"github.com/vapstack/rbi"
	internalroaring "github.com/vapstack/rbi/internal/roaring64/roaring"
)

func makePoolSample(now string, baseline, current rbi.PoolStats) poolSample {
	return poolSample{
		CapturedAt: now,
		Stats:      current,
		Delta:      makePoolDelta(baseline, current),
	}
}

func summarizePools(samples []poolSample, final *poolSample) *poolSummary {
	if len(samples) == 0 && final == nil {
		return nil
	}
	out := &poolSummary{}
	if final != nil {
		out.FinalDelta = final.Delta
	} else {
		out.FinalDelta = samples[len(samples)-1].Delta
	}
	return out
}

func makePoolDelta(baseline, current rbi.PoolStats) poolDelta {
	return poolDelta{
		Roaring64: roaringPoolDelta{
			BitmapContainers:      deltaRoaringPoolCounter(baseline.Roaring64.BitmapContainers, current.Roaring64.BitmapContainers),
			ArrayContainers:       deltaRoaringPoolCounter(baseline.Roaring64.ArrayContainers, current.Roaring64.ArrayContainers),
			ArrayContainerClasses: deltaArrayContainerClassPool(baseline.Roaring64.ArrayContainerClasses, current.Roaring64.ArrayContainerClasses),
			BitmapIterators:       deltaRoaringPoolCounter(baseline.Roaring64.BitmapIterators, current.Roaring64.BitmapIterators),
			ManyBitmapIterators:   deltaRoaringPoolCounter(baseline.Roaring64.ManyBitmapIterators, current.Roaring64.ManyBitmapIterators),
			BitmapIterators64:     deltaRoaringPoolCounter(baseline.Roaring64.BitmapIterators64, current.Roaring64.BitmapIterators64),
			AddManyBatches64:      deltaRoaringSlicePool(baseline.Roaring64.AddManyBatches64, current.Roaring64.AddManyBatches64),
		},
		Scratch: scratchPoolDelta{
			RoaringBitmap:                deltaPoolCounter(baseline.Scratch.RoaringBitmap, current.Scratch.RoaringBitmap),
			IntSlice:                     deltaSlicePool(baseline.Scratch.IntSlice, current.Scratch.IntSlice),
			Uint64Slice:                  deltaSlicePool(baseline.Scratch.Uint64Slice, current.Scratch.Uint64Slice),
			RoaringSlice:                 deltaSlicePool(baseline.Scratch.RoaringSlice, current.Scratch.RoaringSlice),
			PostingListSlice:             deltaSlicePool(baseline.Scratch.PostingListSlice, current.Scratch.PostingListSlice),
			BitmapResultSlice:            deltaSlicePool(baseline.Scratch.BitmapResultSlice, current.Scratch.BitmapResultSlice),
			CountORBranchSlice:           deltaSlicePool(baseline.Scratch.CountORBranchSlice, current.Scratch.CountORBranchSlice),
			PlannerOROrderIterSlice:      deltaSlicePool(baseline.Scratch.PlannerOROrderIterSlice, current.Scratch.PlannerOROrderIterSlice),
			PlannerOROrderMergeItemSlice: deltaSlicePool(baseline.Scratch.PlannerOROrderMergeItemSlice, current.Scratch.PlannerOROrderMergeItemSlice),
		},
		Writes: writePoolDelta{
			UniqueLeavingOuter: deltaMapPool(baseline.Writes.UniqueLeavingOuter, current.Writes.UniqueLeavingOuter),
			UniqueLeavingInner: deltaMapPool(baseline.Writes.UniqueLeavingInner, current.Writes.UniqueLeavingInner),
			UniqueSeenOuter:    deltaMapPool(baseline.Writes.UniqueSeenOuter, current.Writes.UniqueSeenOuter),
			UniqueSeenInner:    deltaMapPool(baseline.Writes.UniqueSeenInner, current.Writes.UniqueSeenInner),
			WriteDeltaOuter:    deltaMapPool(baseline.Writes.WriteDeltaOuter, current.Writes.WriteDeltaOuter),
			WriteDeltaInner:    deltaMapPool(baseline.Writes.WriteDeltaInner, current.Writes.WriteDeltaInner),
		},
	}
}

func deltaPoolCounter(baseline, current rbi.PoolCounterStats) poolCounterDelta {
	return poolCounterDelta{
		Gets:         deltaUint64(current.Gets, baseline.Gets),
		Hits:         deltaUint64(current.Hits, baseline.Hits),
		Misses:       deltaUint64(current.Misses, baseline.Misses),
		Puts:         deltaUint64(current.Puts, baseline.Puts),
		DropNil:      deltaUint64(current.DropNil, baseline.DropNil),
		DropRejected: deltaUint64(current.DropRejected, baseline.DropRejected),
	}
}

func deltaRoaringPoolCounter(baseline, current internalroaring.PoolCounterStats) poolCounterDelta {
	return poolCounterDelta{
		Gets:         deltaUint64(current.Gets, baseline.Gets),
		Hits:         deltaUint64(current.Hits, baseline.Hits),
		Misses:       deltaUint64(current.Misses, baseline.Misses),
		Puts:         deltaUint64(current.Puts, baseline.Puts),
		DropNil:      deltaUint64(current.DropNil, baseline.DropNil),
		DropRejected: deltaUint64(current.DropRejected, baseline.DropRejected),
	}
}

func deltaSlicePool(baseline, current rbi.SlicePoolStats) slicePoolDelta {
	return slicePoolDelta{
		poolCounterDelta:     deltaPoolCounter(baseline.PoolCounterStats, current.PoolCounterStats),
		MaxRequestedCapacity: deltaInt(current.MaxRequestedCapacity, baseline.MaxRequestedCapacity),
		MaxReturnedCapacity:  deltaInt(current.MaxReturnedCapacity, baseline.MaxReturnedCapacity),
	}
}

func deltaMapPool(baseline, current rbi.MapPoolStats) mapPoolDelta {
	return mapPoolDelta{
		poolCounterDelta:   deltaPoolCounter(baseline.PoolCounterStats, current.PoolCounterStats),
		MaxReturnedEntries: deltaInt(current.MaxReturnedEntries, baseline.MaxReturnedEntries),
	}
}

func deltaArrayContainerClassPool(baseline, current internalroaring.ArrayContainerClassPoolStats) arrayContainerClassPoolDelta {
	classes := make([]arrayContainerClassDelta, 0, len(current.Classes))
	for i, cur := range current.Classes {
		base := internalroaring.ArrayContainerClassStats{}
		if i < len(baseline.Classes) && baseline.Classes[i].Capacity == cur.Capacity {
			base = baseline.Classes[i]
		}
		classes = append(classes, arrayContainerClassDelta{
			Capacity: cur.Capacity,
			Gets:     deltaUint64(cur.Gets, base.Gets),
			Hits:     deltaUint64(cur.Hits, base.Hits),
			Misses:   deltaUint64(cur.Misses, base.Misses),
			Puts:     deltaUint64(cur.Puts, base.Puts),
		})
	}
	return arrayContainerClassPoolDelta{
		MaxPooledCapacity:    deltaInt(current.MaxPooledCapacity, baseline.MaxPooledCapacity),
		MaxRequestedCapacity: deltaInt(current.MaxRequestedCapacity, baseline.MaxRequestedCapacity),
		MaxReturnedCapacity:  deltaInt(current.MaxReturnedCapacity, baseline.MaxReturnedCapacity),
		DirectAllocs:         deltaUint64(current.DirectAllocs, baseline.DirectAllocs),
		DirectAllocElements:  deltaUint64(current.DirectAllocElements, baseline.DirectAllocElements),
		DropBelowMinCapacity: deltaUint64(current.DropBelowMinCapacity, baseline.DropBelowMinCapacity),
		DropOutOfRange:       deltaUint64(current.DropOutOfRange, baseline.DropOutOfRange),
		Classes:              classes,
	}
}

func deltaRoaringSlicePool(baseline, current internalroaring.SlicePoolStats) slicePoolDelta {
	return slicePoolDelta{
		poolCounterDelta:     deltaRoaringPoolCounter(baseline.PoolCounterStats, current.PoolCounterStats),
		MaxRequestedCapacity: deltaInt(current.MaxRequestedCapacity, baseline.MaxRequestedCapacity),
		MaxReturnedCapacity:  deltaInt(current.MaxReturnedCapacity, baseline.MaxReturnedCapacity),
	}
}

func deltaInt(current, baseline int) int {
	if current < baseline {
		return 0
	}
	return current - baseline
}
