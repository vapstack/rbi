package roaring64

import (
	"sync"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/roaring64/roaring"
)

type PoolStats struct {
	roaring.PoolStats
	BitmapIterators64 PoolCounterStats `json:"bitmap_iterators64"`
	AddManyBatches64  SlicePoolStats   `json:"add_many_batches64"`
}

type PoolCounterStats = roaring.PoolCounterStats
type SlicePoolStats = roaring.SlicePoolStats

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
	bitmapIterator64PoolStats poolCounterSet
	addManyBatchPoolStats     slicePoolCounterSet

	poolStatsResetMu       sync.RWMutex
	poolStatsResetBaseline PoolStats
	poolStatsResetActive   bool
)

const maxPooledAddManyBatchCapacity = 128 << 10

func GetPoolStats() PoolStats {
	return PoolStats{
		PoolStats:         roaring.GetPoolStats(),
		BitmapIterators64: bitmapIterator64PoolStats.snapshot(),
		AddManyBatches64:  addManyBatchPoolStats.snapshot(maxPooledAddManyBatchCapacity),
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

	maxima := addManyBatchPoolStats.snapshotSinceReset(maxPooledAddManyBatchCapacity)
	return PoolStats{
		PoolStats:         roaring.GetPoolStatsSinceReset(),
		BitmapIterators64: subtractPoolCounterStats(current.BitmapIterators64, baseline.BitmapIterators64),
		AddManyBatches64: SlicePoolStats{
			PoolCounterStats:     subtractPoolCounterStats(current.AddManyBatches64.PoolCounterStats, baseline.AddManyBatches64.PoolCounterStats),
			MaxRetainedCapacity:  maxima.MaxRetainedCapacity,
			MaxRequestedCapacity: maxima.MaxRequestedCapacity,
			MaxReturnedCapacity:  maxima.MaxReturnedCapacity,
		},
	}
}

func ResetPoolStats() {
	poolStatsResetMu.Lock()
	poolStatsResetBaseline = GetPoolStats()
	roaring.ResetPoolStats()
	addManyBatchPoolStats.resetSinceReset()
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
