package wexec

import (
	"slices"
	"sync"
	"time"
)

type scheduler struct {
	stats  statsCounters
	window time.Duration
	maxOps int
	maxQ   int

	queue        []*writeJob
	batchScratch []*writeJob

	mu           sync.Mutex
	cond         *sync.Cond
	waitNotify   chan struct{}
	waitTimer    *time.Timer
	running      bool
	forceWake    bool
	queueHead    int
	queueLen     int
	hotUntil     time.Time
	hotBatchSize int
}

func newScheduler(maxOps int, window time.Duration, maxQueue int, statsEnabled bool) scheduler {
	if maxOps <= 1 || window <= 0 {
		maxOps = 1
		window = 0
	}
	if maxQueue < 0 {
		maxQueue = 0
	}
	capHint := max(64, maxOps*4)
	if maxQueue > 0 && maxQueue < capHint {
		capHint = maxQueue
	}
	return scheduler{
		stats:      statsCounters{Enabled: statsEnabled},
		window:     window,
		maxOps:     maxOps,
		maxQ:       maxQueue,
		waitNotify: make(chan struct{}, 1),
		queue:      make([]*writeJob, capHint),
	}
}

func (s *scheduler) getStats() Stats {
	s.mu.Lock()
	queueLen := s.queueLen
	queueCap := cap(s.queue)
	running := s.running
	hotActive := s.window > 0 && time.Now().Before(s.hotUntil) && (s.hotBatchSize == 0 || s.hotBatchSize >= 3)
	s.mu.Unlock()

	return s.stats.snapshot(s.window, s.maxOps, s.maxQ, queueLen, queueCap, running, hotActive)
}

func (s *scheduler) queueIndex(i int) int {
	idx := s.queueHead + i
	if idx >= len(s.queue) {
		idx -= len(s.queue)
	}
	return idx
}

func (s *scheduler) queueAt(i int) *writeJob {
	return s.queue[s.queueIndex(i)]
}

func (s *scheduler) growQueue() {
	newCap := len(s.queue) * 2
	if newCap < 64 {
		newCap = 64
	}
	if s.maxQ > 0 && newCap > s.maxQ {
		newCap = s.maxQ
	}
	if newCap < s.queueLen+1 {
		newCap = s.queueLen + 1
	}
	grown := make([]*writeJob, newCap)
	for i := 0; i < s.queueLen; i++ {
		grown[i] = s.queueAt(i)
	}
	s.queue = grown
	s.queueHead = 0
}

func (s *scheduler) enqueue(job *writeJob) {
	if s.queueLen == len(s.queue) {
		s.growQueue()
	}
	idx := s.queueHead + s.queueLen
	if idx >= len(s.queue) {
		idx -= len(s.queue)
	}
	s.queue[idx] = job
	s.queueLen++
}

func (s *scheduler) dequeueFrontScratch(n int) []*writeJob {
	batch := slices.Grow(s.batchScratch, n)[:n]
	for i := 0; i < n; i++ {
		idx := s.queueIndex(i)
		batch[i] = s.queue[idx]
		s.queue[idx] = nil
	}
	s.queueHead += n
	if s.queueHead >= len(s.queue) {
		s.queueHead -= len(s.queue)
	}
	s.queueLen -= n
	if s.queueLen == 0 {
		s.queueHead = 0
	}
	s.batchScratch = nil
	return batch
}

func (s *scheduler) frontLimit() int {
	limit := min(s.maxOps, s.queueLen)
	if limit <= 1 {
		return limit
	}
	if s.queueAt(0).isolated {
		return 1
	}
	for i := 1; i < limit; i++ {
		if s.queueAt(i).isolated {
			return i
		}
	}
	return limit
}

func repeatedIDCompatible(prev, req *request) bool {
	return (req.canCoalesceSetDelete() && prev.canCoalesceSetDelete()) || (req.canShareRepeatedID() && prev.canShareRepeatedID())
}

func (s *scheduler) waitDurationLocked(frontLimit int) time.Duration {
	if s.window <= 0 || s.queueLen >= s.maxOps {
		return 0
	}

	now := time.Now()
	switch {

	case frontLimit > 1:
		return s.window

	case s.queueLen == 1 &&
		(s.hotBatchSize == 0 || s.hotBatchSize >= 3) &&
		now.Before(s.hotUntil):
		waitDur := s.window
		if waitDur > 50*time.Microsecond {
			waitDur /= 2
		}
		return waitDur

	default:
		return 0
	}
}

func (s *scheduler) waitWindowLocked(frontLimit int) bool {
	waitDur := s.waitDurationLocked(frontLimit)
	if waitDur <= 0 {
		return true
	}

	start := time.Now()
	s.mu.Unlock()

	if frontLimit > 1 {
		deadline := start.Add(waitDur)
		timer := s.waitTimer

		for {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			if timer == nil {
				timer = time.NewTimer(remaining)
				s.waitTimer = timer
			} else {
				timer.Reset(remaining)
			}

			select {
			case <-timer.C:
			case <-s.waitNotify:
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			s.mu.Lock()
			forceWake := s.forceWake
			if forceWake {
				s.forceWake = false
			}
			if s.queueLen == 0 {
				if s.stats.Enabled {
					s.stats.CoalesceWaits.Add(1)
					s.stats.CoalesceWaitNanos.Add(uint64(time.Since(start)))
				}
				s.running = false
				s.mu.Unlock()
				return false
			}
			if forceWake {
				s.mu.Unlock()
				break
			}
			s.mu.Unlock()
		}

	} else {
		deadline := start.Add(waitDur)
		timer := s.waitTimer

		for {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			if timer == nil {
				timer = time.NewTimer(remaining)
				s.waitTimer = timer
			} else {
				timer.Reset(remaining)
			}

			select {
			case <-timer.C:
			case <-s.waitNotify:
			}

			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			s.mu.Lock()
			forceWake := s.forceWake
			if forceWake {
				s.forceWake = false
			}
			if s.queueLen == 0 {
				if s.stats.Enabled {
					s.stats.CoalesceWaits.Add(1)
					s.stats.CoalesceWaitNanos.Add(uint64(time.Since(start)))
				}
				s.running = false
				s.mu.Unlock()
				return false
			}
			if forceWake {
				s.mu.Unlock()
				break
			}
			if s.hotBatchSize > 0 && s.queueLen >= s.hotBatchSize {
				s.mu.Unlock()
				break
			}
			s.mu.Unlock()
		}
	}

	if s.stats.Enabled {
		s.stats.CoalesceWaits.Add(1)
		s.stats.CoalesceWaitNanos.Add(uint64(time.Since(start)))
	}
	s.mu.Lock()
	s.forceWake = false
	if s.queueLen == 0 {
		s.running = false
		s.mu.Unlock()
		return false
	}
	return true
}

func (s *scheduler) updateHotAfterDequeueLocked(n int, now time.Time) {
	if n > 1 {
		keepHot := 4 * s.window
		if keepHot < 500*time.Microsecond {
			keepHot = 500 * time.Microsecond
		}
		s.hotUntil = now.Add(keepHot)
		s.hotBatchSize = n
		return
	}
	s.hotUntil = time.Time{}
	s.hotBatchSize = 0
}

func (s *scheduler) recordDequeuedStatsLocked(batch []*writeJob, now time.Time) {
	if !s.stats.Enabled {
		return
	}
	nowUnix := now.UnixNano()
	var queueWait uint64
	for _, job := range batch {
		if job.enqueuedAt == 0 || nowUnix <= job.enqueuedAt {
			continue
		}
		queueWait += uint64(nowUnix - job.enqueuedAt)
	}
	s.stats.Dequeued.Add(uint64(len(batch)))
	s.stats.QueueWaitNanos.Add(queueWait)
}

func (s *scheduler) recycleBatchScratchLocked(batch []*writeJob) {
	if cap(batch) > cap(s.batchScratch) {
		s.batchScratch = batch[:0]
	} else if s.batchScratch == nil {
		s.batchScratch = batch[:0]
	}
}

func (s *scheduler) repeatedIDLimitLocked(limit int, strKey bool) int {
	if limit <= 1 {
		return limit
	}

	if strKey {
		lastByID := repeatStringIDPool.Get()
		for i := 0; i < limit; i++ {
			req := s.queueAt(i).reqs[0]
			id := req.id.String()

			prevIdx, seen := lastByID[id]
			if seen {
				prev := s.queueAt(prevIdx).reqs[0]
				if !repeatedIDCompatible(prev, req) {
					limit = i
					break
				}
			}
			lastByID[id] = i
		}
		repeatStringIDPool.Put(lastByID)
		return limit
	}

	lastByID := repeatUintIDPool.Get()
	for i := 0; i < limit; i++ {
		req := s.queueAt(i).reqs[0]
		id := req.id.Uint()

		prevIdx, seen := lastByID[id]
		if seen {
			prev := s.queueAt(prevIdx).reqs[0]
			if !repeatedIDCompatible(prev, req) {
				limit = i
				break
			}
		}
		lastByID[id] = i
	}
	repeatUintIDPool.Put(lastByID)
	return limit
}

func (s *scheduler) popBatch(strKey bool) []*writeJob {
	s.mu.Lock()
	if s.queueLen == 0 {
		s.running = false
		s.mu.Unlock()
		return nil
	}

	frontLimit := s.frontLimit()
	if !s.waitWindowLocked(frontLimit) {
		return nil
	}

	n := s.frontLimit()
	n = s.repeatedIDLimitLocked(n, strKey)

	batch := s.dequeueFrontScratch(n)
	now := time.Now()

	s.updateHotAfterDequeueLocked(n, now)
	if len(batch) > 1 {
		s.markSupersededSetDeleteJobs(batch, strKey)
	}
	s.recordDequeuedStatsLocked(batch, now)
	if s.cond != nil {
		s.cond.Broadcast()
	}
	s.mu.Unlock()

	return batch
}

func (s *scheduler) markSupersededSetDeleteJobs(batch []*writeJob, strKey bool) {
	if len(batch) < 2 {
		return
	}
	stats := s.stats.Enabled

	if strKey {
		lastByID := repeatStringIDPool.Get()
		for i := range batch {
			req := batch[i].reqs[0]
			req.replacedBy = nil
			id := req.id.String()

			prevIdx, seen := lastByID[id]
			if seen && req.canCoalesceSetDelete() {
				prev := batch[prevIdx].reqs[0]
				if prev.canCoalesceSetDelete() {
					prev.replacedBy = req
					if stats {
						s.stats.CoalescedSetDelete.Add(1)
					}
				}
			}
			lastByID[id] = i
		}
		repeatStringIDPool.Put(lastByID)
		return
	}

	lastByID := repeatUintIDPool.Get()
	for i := range batch {
		req := batch[i].reqs[0]
		req.replacedBy = nil
		id := req.id.Uint()

		prevIdx, seen := lastByID[id]
		if seen && req.canCoalesceSetDelete() {
			prev := batch[prevIdx].reqs[0]
			if prev.canCoalesceSetDelete() {
				prev.replacedBy = req
				if stats {
					s.stats.CoalescedSetDelete.Add(1)
				}
			}
		}
		lastByID[id] = i
	}

	repeatUintIDPool.Put(lastByID)
}
