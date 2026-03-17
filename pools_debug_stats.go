//go:build rbidebug

package rbi

import (
	"sync"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/roaring64"
)

type PoolCounterStats struct {
	Gets         uint64
	Hits         uint64
	Misses       uint64
	Puts         uint64
	DropNil      uint64
	DropRejected uint64
}

type SlicePoolStats struct {
	PoolCounterStats
	MaxRetainedCapacity  int
	MaxRequestedCapacity int
	MaxReturnedCapacity  int
}

type MapPoolStats struct {
	PoolCounterStats
	MaxRetainedEntries int
	MaxReturnedEntries int
}

type ScratchPoolStats struct {
	RoaringBitmap                PoolCounterStats
	IntSlice                     SlicePoolStats
	Uint64Slice                  SlicePoolStats
	RoaringSlice                 SlicePoolStats
	PostingListSlice             SlicePoolStats
	BitmapResultSlice            SlicePoolStats
	CountORBranchSlice           SlicePoolStats
	PlannerOROrderIterSlice      SlicePoolStats
	PlannerOROrderMergeItemSlice SlicePoolStats
}

type WritePoolStats struct {
	UniqueLeavingOuter MapPoolStats
	UniqueLeavingInner MapPoolStats
	UniqueSeenOuter    MapPoolStats
	UniqueSeenInner    MapPoolStats
	WriteDeltaOuter    MapPoolStats
	WriteDeltaInner    MapPoolStats
}

type PoolStats struct {
	Roaring64 roaring64.PoolStats
	Scratch   ScratchPoolStats
	Writes    WritePoolStats
}

type poolCounterSet struct {
	gets         atomic.Uint64
	hits         atomic.Uint64
	misses       atomic.Uint64
	puts         atomic.Uint64
	dropNil      atomic.Uint64
	dropRejected atomic.Uint64
}

func (p *poolCounterSet) onGetHit() {
	p.gets.Add(1)
	p.hits.Add(1)
}

func (p *poolCounterSet) onGetMiss() {
	p.gets.Add(1)
	p.misses.Add(1)
}

func (p *poolCounterSet) onPut() {
	p.puts.Add(1)
}

func (p *poolCounterSet) onDropNil() {
	p.dropNil.Add(1)
}

func (p *poolCounterSet) onDropRejected() {
	p.dropRejected.Add(1)
}

func (p *poolCounterSet) snapshot() PoolCounterStats {
	return PoolCounterStats{
		Gets:         p.gets.Load(),
		Hits:         p.hits.Load(),
		Misses:       p.misses.Load(),
		Puts:         p.puts.Load(),
		DropNil:      p.dropNil.Load(),
		DropRejected: p.dropRejected.Load(),
	}
}

type slicePoolCounterSet struct {
	base              poolCounterSet
	maxRequest        atomic.Uint64
	maxReturned       atomic.Uint64
	windowMaxRequest  atomic.Uint64
	windowMaxReturned atomic.Uint64
}

func (s *slicePoolCounterSet) noteRequested(capHint int) {
	atomicMaxUint64(&s.maxRequest, uint64(capHint))
	atomicMaxUint64(&s.windowMaxRequest, uint64(capHint))
}

func (s *slicePoolCounterSet) noteReturned(capVal int) {
	atomicMaxUint64(&s.maxReturned, uint64(capVal))
	atomicMaxUint64(&s.windowMaxReturned, uint64(capVal))
}

func (s *slicePoolCounterSet) snapshot(limit int) SlicePoolStats {
	return SlicePoolStats{
		PoolCounterStats:     s.base.snapshot(),
		MaxRetainedCapacity:  limit,
		MaxRequestedCapacity: int(s.maxRequest.Load()),
		MaxReturnedCapacity:  int(s.maxReturned.Load()),
	}
}

func (s *slicePoolCounterSet) snapshotSinceReset(limit int) SlicePoolStats {
	return SlicePoolStats{
		MaxRetainedCapacity:  limit,
		MaxRequestedCapacity: int(s.windowMaxRequest.Load()),
		MaxReturnedCapacity:  int(s.windowMaxReturned.Load()),
	}
}

func (s *slicePoolCounterSet) resetSinceReset() {
	s.windowMaxRequest.Store(0)
	s.windowMaxReturned.Store(0)
}

type mapPoolCounterSet struct {
	base              poolCounterSet
	maxReturned       atomic.Uint64
	windowMaxReturned atomic.Uint64
}

func (s *mapPoolCounterSet) noteReturned(size int) {
	atomicMaxUint64(&s.maxReturned, uint64(size))
	atomicMaxUint64(&s.windowMaxReturned, uint64(size))
}

func (s *mapPoolCounterSet) snapshot(limit int) MapPoolStats {
	return MapPoolStats{
		PoolCounterStats:   s.base.snapshot(),
		MaxRetainedEntries: limit,
		MaxReturnedEntries: int(s.maxReturned.Load()),
	}
}

func (s *mapPoolCounterSet) snapshotSinceReset(limit int) MapPoolStats {
	return MapPoolStats{
		MaxRetainedEntries: limit,
		MaxReturnedEntries: int(s.windowMaxReturned.Load()),
	}
}

func (s *mapPoolCounterSet) resetSinceReset() {
	s.windowMaxReturned.Store(0)
}

func atomicMaxUint64(dst *atomic.Uint64, v uint64) {
	for {
		cur := dst.Load()
		if v <= cur {
			return
		}
		if dst.CompareAndSwap(cur, v) {
			return
		}
	}
}

var (
	roaringBufPoolStats              poolCounterSet
	intSlicePoolStats                slicePoolCounterSet
	uint64SlicePoolStats             slicePoolCounterSet
	roaringSlicePoolStats            slicePoolCounterSet
	postingListSlicePoolStats        slicePoolCounterSet
	bitmapResultSlicePoolStats       slicePoolCounterSet
	countORBranchSlicePoolStats      slicePoolCounterSet
	plannerOROrderIterSlicePoolStats slicePoolCounterSet
	plannerOROrderMergeItemPoolStats slicePoolCounterSet

	uniqueLeavingOuterMapStats mapPoolCounterSet
	uniqueLeavingInnerMapStats mapPoolCounterSet
	uniqueSeenOuterMapStats    mapPoolCounterSet
	uniqueSeenInnerMapStats    mapPoolCounterSet
	writeDeltaOuterMapStats    mapPoolCounterSet
	writeDeltaInnerMapStats    mapPoolCounterSet

	poolStatsResetMu       sync.RWMutex
	poolStatsResetBaseline PoolStats
	poolStatsResetActive   bool
)

func snapshotPoolStats() PoolStats {
	return PoolStats{
		Roaring64: roaring64.GetPoolStats(),
		Scratch: ScratchPoolStats{
			RoaringBitmap:                roaringBufPoolStats.snapshot(),
			IntSlice:                     intSlicePoolStats.snapshot(intSlicePoolMaxCap),
			Uint64Slice:                  uint64SlicePoolStats.snapshot(uint64SlicePoolMaxCap),
			RoaringSlice:                 roaringSlicePoolStats.snapshot(roaringSlicePoolMaxCap),
			PostingListSlice:             postingListSlicePoolStats.snapshot(postingListSlicePoolMaxCap),
			BitmapResultSlice:            bitmapResultSlicePoolStats.snapshot(bitmapResultSlicePoolMaxCap),
			CountORBranchSlice:           countORBranchSlicePoolStats.snapshot(countORBranchSlicePoolMaxCap),
			PlannerOROrderIterSlice:      plannerOROrderIterSlicePoolStats.snapshot(plannerOROrderIterSlicePoolMaxCap),
			PlannerOROrderMergeItemSlice: plannerOROrderMergeItemPoolStats.snapshot(plannerOROrderMergeItemSliceMaxCap),
		},
		Writes: WritePoolStats{
			UniqueLeavingOuter: uniqueLeavingOuterMapStats.snapshot(pooledUniqueOuterMaxLen),
			UniqueLeavingInner: uniqueLeavingInnerMapStats.snapshot(pooledUniqueInnerMaxLen),
			UniqueSeenOuter:    uniqueSeenOuterMapStats.snapshot(pooledUniqueOuterMaxLen),
			UniqueSeenInner:    uniqueSeenInnerMapStats.snapshot(pooledUniqueInnerMaxLen),
			WriteDeltaOuter:    writeDeltaOuterMapStats.snapshot(pooledWriteDeltaOuterMaxLen),
			WriteDeltaInner:    writeDeltaInnerMapStats.snapshot(pooledWriteDeltaInnerMaxLen),
		},
	}
}

func resetPoolStats() {
	poolStatsResetMu.Lock()
	poolStatsResetBaseline = snapshotPoolStats()
	roaring64.ResetPoolStats()
	resetPoolStatWindows()
	poolStatsResetActive = true
	poolStatsResetMu.Unlock()
}

func snapshotPoolStatsSinceReset() PoolStats {
	current := snapshotPoolStats()

	poolStatsResetMu.RLock()
	baseline := poolStatsResetBaseline
	active := poolStatsResetActive
	poolStatsResetMu.RUnlock()
	if !active {
		return current
	}

	return PoolStats{
		Roaring64: roaring64.GetPoolStatsSinceReset(),
		Scratch: ScratchPoolStats{
			RoaringBitmap:                subtractPoolCounterStats(current.Scratch.RoaringBitmap, baseline.Scratch.RoaringBitmap),
			IntSlice:                     subtractSlicePoolStatsSinceReset(current.Scratch.IntSlice, baseline.Scratch.IntSlice, intSlicePoolStats.snapshotSinceReset(intSlicePoolMaxCap)),
			Uint64Slice:                  subtractSlicePoolStatsSinceReset(current.Scratch.Uint64Slice, baseline.Scratch.Uint64Slice, uint64SlicePoolStats.snapshotSinceReset(uint64SlicePoolMaxCap)),
			RoaringSlice:                 subtractSlicePoolStatsSinceReset(current.Scratch.RoaringSlice, baseline.Scratch.RoaringSlice, roaringSlicePoolStats.snapshotSinceReset(roaringSlicePoolMaxCap)),
			PostingListSlice:             subtractSlicePoolStatsSinceReset(current.Scratch.PostingListSlice, baseline.Scratch.PostingListSlice, postingListSlicePoolStats.snapshotSinceReset(postingListSlicePoolMaxCap)),
			BitmapResultSlice:            subtractSlicePoolStatsSinceReset(current.Scratch.BitmapResultSlice, baseline.Scratch.BitmapResultSlice, bitmapResultSlicePoolStats.snapshotSinceReset(bitmapResultSlicePoolMaxCap)),
			CountORBranchSlice:           subtractSlicePoolStatsSinceReset(current.Scratch.CountORBranchSlice, baseline.Scratch.CountORBranchSlice, countORBranchSlicePoolStats.snapshotSinceReset(countORBranchSlicePoolMaxCap)),
			PlannerOROrderIterSlice:      subtractSlicePoolStatsSinceReset(current.Scratch.PlannerOROrderIterSlice, baseline.Scratch.PlannerOROrderIterSlice, plannerOROrderIterSlicePoolStats.snapshotSinceReset(plannerOROrderIterSlicePoolMaxCap)),
			PlannerOROrderMergeItemSlice: subtractSlicePoolStatsSinceReset(current.Scratch.PlannerOROrderMergeItemSlice, baseline.Scratch.PlannerOROrderMergeItemSlice, plannerOROrderMergeItemPoolStats.snapshotSinceReset(plannerOROrderMergeItemSliceMaxCap)),
		},
		Writes: WritePoolStats{
			UniqueLeavingOuter: subtractMapPoolStatsSinceReset(current.Writes.UniqueLeavingOuter, baseline.Writes.UniqueLeavingOuter, uniqueLeavingOuterMapStats.snapshotSinceReset(pooledUniqueOuterMaxLen)),
			UniqueLeavingInner: subtractMapPoolStatsSinceReset(current.Writes.UniqueLeavingInner, baseline.Writes.UniqueLeavingInner, uniqueLeavingInnerMapStats.snapshotSinceReset(pooledUniqueInnerMaxLen)),
			UniqueSeenOuter:    subtractMapPoolStatsSinceReset(current.Writes.UniqueSeenOuter, baseline.Writes.UniqueSeenOuter, uniqueSeenOuterMapStats.snapshotSinceReset(pooledUniqueOuterMaxLen)),
			UniqueSeenInner:    subtractMapPoolStatsSinceReset(current.Writes.UniqueSeenInner, baseline.Writes.UniqueSeenInner, uniqueSeenInnerMapStats.snapshotSinceReset(pooledUniqueInnerMaxLen)),
			WriteDeltaOuter:    subtractMapPoolStatsSinceReset(current.Writes.WriteDeltaOuter, baseline.Writes.WriteDeltaOuter, writeDeltaOuterMapStats.snapshotSinceReset(pooledWriteDeltaOuterMaxLen)),
			WriteDeltaInner:    subtractMapPoolStatsSinceReset(current.Writes.WriteDeltaInner, baseline.Writes.WriteDeltaInner, writeDeltaInnerMapStats.snapshotSinceReset(pooledWriteDeltaInnerMaxLen)),
		},
	}
}

func subtractPoolCounterStats(current, baseline PoolCounterStats) PoolCounterStats {
	return PoolCounterStats{
		Gets:         subtractUint64(current.Gets, baseline.Gets),
		Hits:         subtractUint64(current.Hits, baseline.Hits),
		Misses:       subtractUint64(current.Misses, baseline.Misses),
		Puts:         subtractUint64(current.Puts, baseline.Puts),
		DropNil:      subtractUint64(current.DropNil, baseline.DropNil),
		DropRejected: subtractUint64(current.DropRejected, baseline.DropRejected),
	}
}

func subtractSlicePoolStatsSinceReset(current, baseline, maxima SlicePoolStats) SlicePoolStats {
	return SlicePoolStats{
		PoolCounterStats:     subtractPoolCounterStats(current.PoolCounterStats, baseline.PoolCounterStats),
		MaxRetainedCapacity:  maxima.MaxRetainedCapacity,
		MaxRequestedCapacity: maxima.MaxRequestedCapacity,
		MaxReturnedCapacity:  maxima.MaxReturnedCapacity,
	}
}

func subtractMapPoolStatsSinceReset(current, baseline, maxima MapPoolStats) MapPoolStats {
	return MapPoolStats{
		PoolCounterStats:   subtractPoolCounterStats(current.PoolCounterStats, baseline.PoolCounterStats),
		MaxRetainedEntries: maxima.MaxRetainedEntries,
		MaxReturnedEntries: maxima.MaxReturnedEntries,
	}
}

func subtractUint64(current, baseline uint64) uint64 {
	if current < baseline {
		return 0
	}
	return current - baseline
}

func resetPoolStatWindows() {
	intSlicePoolStats.resetSinceReset()
	uint64SlicePoolStats.resetSinceReset()
	roaringSlicePoolStats.resetSinceReset()
	postingListSlicePoolStats.resetSinceReset()
	bitmapResultSlicePoolStats.resetSinceReset()
	countORBranchSlicePoolStats.resetSinceReset()
	plannerOROrderIterSlicePoolStats.resetSinceReset()
	plannerOROrderMergeItemPoolStats.resetSinceReset()

	uniqueLeavingOuterMapStats.resetSinceReset()
	uniqueLeavingInnerMapStats.resetSinceReset()
	uniqueSeenOuterMapStats.resetSinceReset()
	uniqueSeenInnerMapStats.resetSinceReset()
	writeDeltaOuterMapStats.resetSinceReset()
	writeDeltaInnerMapStats.resetSinceReset()
}
