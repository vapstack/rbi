//go:build rbidebug

package roaring

import (
	"sync"
	"sync/atomic"
)

type PoolCounterStats struct {
	Gets         uint64
	Hits         uint64
	Misses       uint64
	Puts         uint64
	DropNil      uint64
	DropRejected uint64
}

type ArrayContainerClassStats struct {
	Capacity int
	Gets     uint64
	Hits     uint64
	Misses   uint64
	Puts     uint64
}

type ArrayContainerClassPoolStats struct {
	MaxPooledCapacity    int
	MaxRequestedCapacity int
	MaxReturnedCapacity  int
	DirectAllocs         uint64
	DirectAllocElements  uint64
	DropBelowMinCapacity uint64
	DropOutOfRange       uint64
	Classes              []ArrayContainerClassStats
}

type SlicePoolStats struct {
	PoolCounterStats
	MaxRetainedCapacity  int
	MaxRequestedCapacity int
	MaxReturnedCapacity  int
}

type PoolStats struct {
	Bitmaps               PoolCounterStats
	BitmapContainers      PoolCounterStats
	ArrayContainers       PoolCounterStats
	ArrayContainerClasses ArrayContainerClassPoolStats
	RunContainers         SlicePoolStats
	BitmapIterators       PoolCounterStats
	ManyBitmapIterators   PoolCounterStats
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

type arrayContainerClassCounterSet struct {
	gets   atomic.Uint64
	hits   atomic.Uint64
	misses atomic.Uint64
	puts   atomic.Uint64
}

func (c *arrayContainerClassCounterSet) onGetHit() {
	c.gets.Add(1)
	c.hits.Add(1)
}

func (c *arrayContainerClassCounterSet) onGetMiss() {
	c.gets.Add(1)
	c.misses.Add(1)
}

func (c *arrayContainerClassCounterSet) onPut() {
	c.puts.Add(1)
}

type arrayContainerClassPoolCounterSet struct {
	maxRequestedCapacity atomic.Uint64
	maxReturnedCapacity  atomic.Uint64
	windowMaxRequested   atomic.Uint64
	windowMaxReturned    atomic.Uint64
	directAllocs         atomic.Uint64
	directAllocElements  atomic.Uint64
	dropBelowMinCapacity atomic.Uint64
	dropOutOfRange       atomic.Uint64
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

func (s *arrayContainerClassPoolCounterSet) noteRequestedCapacity(size int) {
	atomicMaxUint64(&s.maxRequestedCapacity, uint64(size))
	atomicMaxUint64(&s.windowMaxRequested, uint64(size))
}

func (s *arrayContainerClassPoolCounterSet) noteReturnedCapacity(size int) {
	atomicMaxUint64(&s.maxReturnedCapacity, uint64(size))
	atomicMaxUint64(&s.windowMaxReturned, uint64(size))
}

func (s *arrayContainerClassPoolCounterSet) noteDirectAlloc(size int) {
	s.directAllocs.Add(1)
	s.directAllocElements.Add(uint64(size))
}

func (s *arrayContainerClassPoolCounterSet) snapshotSinceReset(limit int) ArrayContainerClassPoolStats {
	return ArrayContainerClassPoolStats{
		MaxPooledCapacity:    limit,
		MaxRequestedCapacity: int(s.windowMaxRequested.Load()),
		MaxReturnedCapacity:  int(s.windowMaxReturned.Load()),
	}
}

func (s *arrayContainerClassPoolCounterSet) resetSinceReset() {
	s.windowMaxRequested.Store(0)
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
	bitmapPoolStats          poolCounterSet
	bitmapContainerPoolStats poolCounterSet
	arrayContainerPoolStats  poolCounterSet
	runContainerPoolStats    slicePoolCounterSet

	arrayContainerClassPoolStats arrayContainerClassPoolCounterSet
	arrayContainerClassStats     [len(pooledArrayContainerCapacities)]arrayContainerClassCounterSet
	bitmapIteratorPoolStats      poolCounterSet
	manyBitmapIteratorPoolStats  poolCounterSet

	poolStatsResetMu       sync.RWMutex
	poolStatsResetBaseline PoolStats
	poolStatsResetActive   bool
)

func GetPoolStats() PoolStats {
	classes := make([]ArrayContainerClassStats, 0, len(pooledArrayContainerCapacities))
	for i, class := range pooledArrayContainerCapacities {
		stats := &arrayContainerClassStats[i]
		classes = append(classes, ArrayContainerClassStats{
			Capacity: class,
			Gets:     stats.gets.Load(),
			Hits:     stats.hits.Load(),
			Misses:   stats.misses.Load(),
			Puts:     stats.puts.Load(),
		})
	}
	return PoolStats{
		Bitmaps:          bitmapPoolStats.snapshot(),
		BitmapContainers: bitmapContainerPoolStats.snapshot(),
		ArrayContainers:  arrayContainerPoolStats.snapshot(),
		ArrayContainerClasses: ArrayContainerClassPoolStats{
			MaxPooledCapacity:    maxPooledArrayContainerCapacity,
			MaxRequestedCapacity: int(arrayContainerClassPoolStats.maxRequestedCapacity.Load()),
			MaxReturnedCapacity:  int(arrayContainerClassPoolStats.maxReturnedCapacity.Load()),
			DirectAllocs:         arrayContainerClassPoolStats.directAllocs.Load(),
			DirectAllocElements:  arrayContainerClassPoolStats.directAllocElements.Load(),
			DropBelowMinCapacity: arrayContainerClassPoolStats.dropBelowMinCapacity.Load(),
			DropOutOfRange:       arrayContainerClassPoolStats.dropOutOfRange.Load(),
			Classes:              classes,
		},
		RunContainers:       runContainerPoolStats.snapshot(maxPooledRunContainerCapacity),
		BitmapIterators:     bitmapIteratorPoolStats.snapshot(),
		ManyBitmapIterators: manyBitmapIteratorPoolStats.snapshot(),
	}
}

func GetPoolStatsSinceReset() PoolStats {
	current := GetPoolStats()

	poolStatsResetMu.RLock()
	baseline := poolStatsResetBaseline
	active := poolStatsResetActive
	poolStatsResetMu.RUnlock()
	if !active {
		return current
	}

	classes := make([]ArrayContainerClassStats, 0, len(current.ArrayContainerClasses.Classes))
	for i, cur := range current.ArrayContainerClasses.Classes {
		base := ArrayContainerClassStats{}
		if i < len(baseline.ArrayContainerClasses.Classes) && baseline.ArrayContainerClasses.Classes[i].Capacity == cur.Capacity {
			base = baseline.ArrayContainerClasses.Classes[i]
		}
		classes = append(classes, ArrayContainerClassStats{
			Capacity: cur.Capacity,
			Gets:     subtractUint64(cur.Gets, base.Gets),
			Hits:     subtractUint64(cur.Hits, base.Hits),
			Misses:   subtractUint64(cur.Misses, base.Misses),
			Puts:     subtractUint64(cur.Puts, base.Puts),
		})
	}

	maxima := arrayContainerClassPoolStats.snapshotSinceReset(maxPooledArrayContainerCapacity)
	runMaxima := runContainerPoolStats.snapshotSinceReset(maxPooledRunContainerCapacity)
	return PoolStats{
		Bitmaps:          subtractPoolCounterStats(current.Bitmaps, baseline.Bitmaps),
		BitmapContainers: subtractPoolCounterStats(current.BitmapContainers, baseline.BitmapContainers),
		ArrayContainers:  subtractPoolCounterStats(current.ArrayContainers, baseline.ArrayContainers),
		ArrayContainerClasses: ArrayContainerClassPoolStats{
			MaxPooledCapacity:    maxima.MaxPooledCapacity,
			MaxRequestedCapacity: maxima.MaxRequestedCapacity,
			MaxReturnedCapacity:  maxima.MaxReturnedCapacity,
			DirectAllocs:         subtractUint64(current.ArrayContainerClasses.DirectAllocs, baseline.ArrayContainerClasses.DirectAllocs),
			DirectAllocElements:  subtractUint64(current.ArrayContainerClasses.DirectAllocElements, baseline.ArrayContainerClasses.DirectAllocElements),
			DropBelowMinCapacity: subtractUint64(current.ArrayContainerClasses.DropBelowMinCapacity, baseline.ArrayContainerClasses.DropBelowMinCapacity),
			DropOutOfRange:       subtractUint64(current.ArrayContainerClasses.DropOutOfRange, baseline.ArrayContainerClasses.DropOutOfRange),
			Classes:              classes,
		},
		RunContainers: SlicePoolStats{
			PoolCounterStats:     subtractPoolCounterStats(current.RunContainers.PoolCounterStats, baseline.RunContainers.PoolCounterStats),
			MaxRetainedCapacity:  runMaxima.MaxRetainedCapacity,
			MaxRequestedCapacity: runMaxima.MaxRequestedCapacity,
			MaxReturnedCapacity:  runMaxima.MaxReturnedCapacity,
		},
		BitmapIterators:     subtractPoolCounterStats(current.BitmapIterators, baseline.BitmapIterators),
		ManyBitmapIterators: subtractPoolCounterStats(current.ManyBitmapIterators, baseline.ManyBitmapIterators),
	}
}

func ResetPoolStats() {
	poolStatsResetMu.Lock()
	poolStatsResetBaseline = GetPoolStats()
	arrayContainerClassPoolStats.resetSinceReset()
	runContainerPoolStats.resetSinceReset()
	poolStatsResetActive = true
	poolStatsResetMu.Unlock()
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

func subtractUint64(current, baseline uint64) uint64 {
	if current < baseline {
		return 0
	}
	return current - baseline
}
