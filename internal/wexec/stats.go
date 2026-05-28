package wexec

import (
	"sync/atomic"
	"time"
)

type Stats struct {
	Window          time.Duration
	MaxBatch        int
	MaxQueue        int
	QueueLen        int
	QueueCap        int
	WorkerRunning   bool
	HotWindowActive bool

	Submitted      uint64
	Enqueued       uint64
	Dequeued       uint64
	QueueHighWater uint64

	ExecutedBatches     uint64
	MultiRequestBatches uint64
	MultiRequestOps     uint64
	BatchSize1          uint64
	BatchSize2To4       uint64
	BatchSize5To8       uint64
	BatchSize9Plus      uint64
	AvgBatchSize        float64
	MaxBatchSeen        uint64

	CallbackOps        uint64
	CoalescedSetDelete uint64

	CoalesceWaits    uint64
	CoalesceWaitTime time.Duration
	QueueWaitTime    time.Duration
	ExecuteTime      time.Duration

	FallbackClosed uint64

	UniqueRejected uint64
	TxBeginErrors  uint64
	TxOpErrors     uint64
	TxCommitErrors uint64
	CallbackErrors uint64
}

type statsCounters struct {
	Enabled bool

	Submitted           atomic.Uint64
	Enqueued            atomic.Uint64
	Dequeued            atomic.Uint64
	ExecutedBatches     atomic.Uint64
	MultiRequestBatches atomic.Uint64
	MultiRequestOps     atomic.Uint64
	BatchSize1          atomic.Uint64
	BatchSize2To4       atomic.Uint64
	BatchSize5To8       atomic.Uint64
	BatchSize9Plus      atomic.Uint64
	CallbackOps         atomic.Uint64
	CoalescedSetDelete  atomic.Uint64
	MaxBatchSeen        atomic.Uint64
	QueueHighWater      atomic.Uint64
	CoalesceWaits       atomic.Uint64
	CoalesceWaitNanos   atomic.Uint64
	QueueWaitNanos      atomic.Uint64
	ExecuteNanos        atomic.Uint64

	FallbackClosed atomic.Uint64

	UniqueRejected atomic.Uint64
	TxBeginErrors  atomic.Uint64
	TxOpErrors     atomic.Uint64
	TxCommitErrors atomic.Uint64
	CallbackErrors atomic.Uint64
}

func (c *statsCounters) recordExecuted(size int) {
	if !c.Enabled {
		return
	}
	c.ExecutedBatches.Add(1)
	if size > 1 {
		c.MultiRequestBatches.Add(1)
		c.MultiRequestOps.Add(uint64(size))
	}
	switch {
	case size <= 1:
		c.BatchSize1.Add(1)
	case size <= 4:
		c.BatchSize2To4.Add(1)
	case size <= 8:
		c.BatchSize5To8.Add(1)
	default:
		c.BatchSize9Plus.Add(1)
	}
	atomicSetMax(&c.MaxBatchSeen, uint64(size))
}

func (c *statsCounters) snapshot(window time.Duration, maxOps, maxQ, queueLen, queueCap int, running, hotActive bool) Stats {
	if !c.Enabled {
		return Stats{}
	}
	out := Stats{
		Window:              window,
		MaxBatch:            maxOps,
		MaxQueue:            maxQ,
		QueueLen:            queueLen,
		QueueCap:            queueCap,
		WorkerRunning:       running,
		HotWindowActive:     hotActive,
		Submitted:           c.Submitted.Load(),
		Enqueued:            c.Enqueued.Load(),
		Dequeued:            c.Dequeued.Load(),
		QueueHighWater:      c.QueueHighWater.Load(),
		ExecutedBatches:     c.ExecutedBatches.Load(),
		MultiRequestBatches: c.MultiRequestBatches.Load(),
		MultiRequestOps:     c.MultiRequestOps.Load(),
		BatchSize1:          c.BatchSize1.Load(),
		BatchSize2To4:       c.BatchSize2To4.Load(),
		BatchSize5To8:       c.BatchSize5To8.Load(),
		BatchSize9Plus:      c.BatchSize9Plus.Load(),
		MaxBatchSeen:        c.MaxBatchSeen.Load(),
		CallbackOps:         c.CallbackOps.Load(),
		CoalescedSetDelete:  c.CoalescedSetDelete.Load(),
		CoalesceWaits:       c.CoalesceWaits.Load(),
		CoalesceWaitTime:    time.Duration(c.CoalesceWaitNanos.Load()),
		QueueWaitTime:       time.Duration(c.QueueWaitNanos.Load()),
		ExecuteTime:         time.Duration(c.ExecuteNanos.Load()),
		FallbackClosed:      c.FallbackClosed.Load(),
		UniqueRejected:      c.UniqueRejected.Load(),
		TxBeginErrors:       c.TxBeginErrors.Load(),
		TxOpErrors:          c.TxOpErrors.Load(),
		TxCommitErrors:      c.TxCommitErrors.Load(),
		CallbackErrors:      c.CallbackErrors.Load(),
	}
	if out.ExecutedBatches > 0 {
		out.AvgBatchSize = float64(out.Dequeued) / float64(out.ExecutedBatches)
	}
	return out
}

func atomicSetMax(dst *atomic.Uint64, v uint64) {
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
