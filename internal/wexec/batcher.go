package wexec

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbierrors"
	"github.com/vapstack/rbi/rbistats"
	"go.etcd.io/bbolt"
)

type Batcher struct {
	sched scheduler

	bolt         *bbolt.DB
	dataBucket   []byte
	strmapBucket []byte

	bucketFillPercent  float64
	rejectEmptyPayload bool

	strKey           bool
	unavailable      func() error
	publishMu        *sync.RWMutex
	commit           func(*bbolt.Tx) error
	indexed          bool
	ops              *RecordOps
	schema           *schema.Schema
	unique           UniqueContext
	snapshotOps      SnapshotOps
	publishCommitted func(uint64, string, *snapshot.View) error
}

type Config struct {
	MaxOps       int
	Window       time.Duration
	MaxQueue     int
	StatsEnabled bool
	Unavailable  func() error

	Bolt         *bbolt.DB
	DataBucket   []byte
	StrMapBucket []byte
	StrKey       bool

	BucketFillPercent  float64
	RejectEmptyPayload bool

	PublishMu        *sync.RWMutex
	Indexed          bool
	Ops              *RecordOps
	Schema           *schema.Schema
	Unique           UniqueContext
	SnapshotOps      SnapshotOps
	PublishCommitted func(uint64, string, *snapshot.View) error
}

func NewBatcher(cfg Config) *Batcher {
	ex := &Batcher{
		sched: newScheduler(cfg.MaxOps, cfg.Window, cfg.MaxQueue, cfg.StatsEnabled),

		bolt:               cfg.Bolt,
		dataBucket:         cfg.DataBucket,
		bucketFillPercent:  cfg.BucketFillPercent,
		rejectEmptyPayload: cfg.RejectEmptyPayload,
		strKey:             cfg.StrKey,
		strmapBucket:       cfg.StrMapBucket,
		unavailable:        cfg.Unavailable,
		publishMu:          cfg.PublishMu,
		commit:             (*bbolt.Tx).Commit,
		indexed:            cfg.Indexed,
		ops:                cfg.Ops,
		schema:             cfg.Schema,
		unique:             cfg.Unique,
		snapshotOps:        cfg.SnapshotOps,
		publishCommitted:   cfg.PublishCommitted,
	}
	ex.sched.cond = sync.NewCond(&ex.sched.mu)
	return ex
}

// OverridePublishCommittedForTest exposes the publish hook to cross-package tests without reflection.
func OverridePublishCommittedForTest(b *Batcher, fn func(uint64, string, *snapshot.View) error) func(uint64, string, *snapshot.View) error {
	old := b.publishCommitted
	b.publishCommitted = fn
	return old
}

func (b *Batcher) BasicStats() (queueLen int, queueMax uint64, executed uint64, dequeued uint64) {
	b.sched.mu.Lock()
	queueLen = b.sched.queueLen
	b.sched.mu.Unlock()
	return queueLen, b.sched.stats.QueueHighWater.Load(), b.sched.stats.ExecutedBatches.Load(), b.sched.stats.Dequeued.Load()
}

func (b *Batcher) Stats() rbistats.AutoBatch {
	return b.sched.getStats()
}

func (b *Batcher) WakeWaiters() {
	b.sched.mu.Lock()
	b.sched.forceWake = true
	b.sched.cond.Broadcast()
	b.sched.mu.Unlock()
	select {
	case b.sched.waitNotify <- struct{}{}:
	default:
	}
}

func (b *Batcher) submit(reqs []*request, isolated bool) error {
	if len(reqs) == 0 {
		return nil
	}

	stats := b.sched.stats.Enabled
	if stats {
		b.sched.stats.Submitted.Add(1)
	}
	if err := b.unavailable(); err != nil {
		for i := 0; i < len(reqs); i++ {
			requestPool.Put(reqs[i])
		}
		if stats {
			b.sched.stats.FallbackClosed.Add(1)
		}
		return err
	}

	if b.sched.maxOps == 1 && b.sched.window == 0 && b.sched.maxQ == 0 {
		b.sched.mu.Lock()
		if !b.sched.running && b.sched.queueLen == 0 {
			if err := b.unavailable(); err != nil {
				b.sched.mu.Unlock()
				for i := 0; i < len(reqs); i++ {
					requestPool.Put(reqs[i])
				}
				if stats {
					b.sched.stats.FallbackClosed.Add(1)
				}
				return err
			}
			b.sched.running = true
			if stats {
				b.sched.stats.Enqueued.Add(1)
				b.sched.stats.Dequeued.Add(1)
				atomicSetMax(&b.sched.stats.QueueHighWater, 1)
			}
			b.sched.mu.Unlock()

			var err error
			if err = b.unavailable(); err != nil {
				assignRequestErr(reqs, err)
			} else {
				b.executeDirect(reqs, isolated || len(reqs) != 1)
				err = firstRequestErr(reqs)
			}
			for i := 0; i < len(reqs); i++ {
				requestPool.Put(reqs[i])
			}

			b.sched.mu.Lock()
			if b.sched.queueLen == 0 {
				b.sched.running = false
				b.sched.cond.Broadcast()
			} else {
				go b.runLoop()
			}
			b.sched.mu.Unlock()
			return err
		}
		b.sched.mu.Unlock()
	}

	job := jobPool.Get()
	job.reqs = reqs
	job.isolated = isolated || len(reqs) != 1

	b.sched.mu.Lock()
	for {
		if err := b.unavailable(); err != nil {
			b.sched.mu.Unlock()
			for i := 0; i < len(reqs); i++ {
				requestPool.Put(reqs[i])
			}
			jobPool.Put(job)
			if stats {
				b.sched.stats.FallbackClosed.Add(1)
			}
			return err
		}
		if !b.sched.running {
			b.sched.running = true
			go b.runLoop()
		}
		if b.sched.maxQ <= 0 || b.sched.queueLen < b.sched.maxQ {
			break
		}
		b.sched.cond.Wait()
	}

	b.sched.enqueue(job)
	select {
	case b.sched.waitNotify <- struct{}{}:
	default:
	}
	if b.sched.stats.Enabled {
		job.enqueuedAt = time.Now().UnixNano()
		b.sched.stats.Enqueued.Add(1)
		atomicSetMax(&b.sched.stats.QueueHighWater, uint64(b.sched.queueLen))
	}
	b.sched.mu.Unlock()

	err := <-job.done
	for i := 0; i < len(reqs); i++ {
		requestPool.Put(reqs[i])
	}
	jobPool.Put(job)
	return err
}

func (b *Batcher) executeDirect(reqs []*request, isolated bool) {
	b.sched.stats.recordExecuted(1)
	stats := b.sched.stats.Enabled
	var started time.Time
	if stats {
		started = time.Now()
	}

	if isolated {
		b.runAtomic(reqs)
	} else {
		b.runShared(reqs)
	}

	if stats {
		b.sched.stats.ExecuteNanos.Add(uint64(time.Since(started)))
	}
}

func (b *Batcher) runLoop() {
	for {
		batch := b.sched.popBatch(b.strKey)
		if len(batch) == 0 {
			return
		}
		if err := b.unavailable(); err != nil {
			failJobs(batch, err)
			clear(batch)
			b.sched.mu.Lock()
			b.sched.recycleBatchScratchLocked(batch)
			b.sched.mu.Unlock()
			continue
		}
		b.executeJobs(batch)
		clear(batch)
		b.sched.mu.Lock()
		b.sched.recycleBatchScratchLocked(batch)
		b.sched.mu.Unlock()
	}
}

func (b *Batcher) executeJobs(batch []*writeJob) {
	if len(batch) == 1 {
		job := batch[0]
		b.executeDirect(job.reqs, job.isolated)
		finishJobs(batch)
		return
	}

	b.sched.stats.recordExecuted(len(batch))
	stats := b.sched.stats.Enabled
	var started time.Time
	if stats {
		started = time.Now()
	}

	reqScratch := requestScratchPool.Get(len(batch))

	for _, job := range batch {
		reqs := job.reqs
		if len(reqs) != 1 {
			assignRequestErr(reqs, fmt.Errorf("internal auto-batch error: mixed grouped request in shared batch"))
			continue
		}
		reqScratch = append(reqScratch, reqs[0])
	}
	if len(reqScratch) != 0 {
		b.runShared(reqScratch)
	}
	finishJobs(batch)
	requestScratchPool.Put(reqScratch)
	if stats {
		b.sched.stats.ExecuteNanos.Add(uint64(time.Since(started)))
	}
}

func (b *Batcher) runShared(batch []*request) {
	for i := 0; i < len(batch); i++ {
		batch[i].Err = nil
	}

	activeScratch := batch
	var ownedScratch []*request
	for i := 0; i < len(batch); i++ {
		if batch[i].replacedBy != nil {
			ownedScratch = requestScratchPool.Get(len(batch))
			activeScratch = ownedScratch
			for j := 0; j < len(batch); j++ {
				req := batch[j]
				if req.replacedBy == nil {
					activeScratch = append(activeScratch, req)
				}
			}
			break
		}
	}

	for {
		retryWithoutReq, done, fatalErr := b.attempt(activeScratch, false)
		if fatalErr != nil {
			assignRequestErr(batch, fatalErr)
			break
		}
		if done {
			break
		}

		removed := false
		if ownedScratch != nil {
			write := 0
			for i := 0; i < len(activeScratch); i++ {
				req := activeScratch[i]
				if !removed && req == retryWithoutReq {
					removed = true
					continue
				}
				if write != i {
					activeScratch[write] = req
				}
				write++
			}
			activeScratch = activeScratch[:write]

		} else {
			ownedScratch = requestScratchPool.Get(len(activeScratch) - 1)
			scratch := ownedScratch
			for i := 0; i < len(activeScratch); i++ {
				req := activeScratch[i]
				if !removed && req == retryWithoutReq {
					removed = true
					continue
				}
				scratch = append(scratch, req)
			}
			activeScratch = scratch
		}
		if !removed {
			assignRequestErr(batch, fmt.Errorf("internal auto-batch retry error: failed request not found"))
			break
		}
		if len(activeScratch) == 0 {
			break
		}
		for i := 0; i < len(activeScratch); i++ {
			req := activeScratch[i]
			if errors.Is(req.Err, rbierrors.ErrUniqueViolation) {
				req.Err = nil
			}
		}
	}
	if ownedScratch != nil {
		requestScratchPool.Put(ownedScratch)
	}
}

func (b *Batcher) runAtomic(batch []*request) {
	for i := 0; i < len(batch); i++ {
		req := batch[i]
		req.Err = nil
		req.replacedBy = nil
	}
	_, done, fatalErr := b.attempt(batch, true)
	if fatalErr != nil {
		assignRequestErr(batch, fatalErr)
		return
	}
	if !done {
		assignRequestErr(batch, fmt.Errorf("internal auto-batch atomic retry error"))
	}
}
