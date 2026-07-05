package rbi

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/engine"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/wexec"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

type writeUnit struct {
	root     *rootStore
	done     chan error
	segments []writeSegment
	truncate *collection
	work     int
	limit    int

	generatedCollections []*collection
	hookTx               *Tx
}

type writeScheduler struct {
	mu       sync.Mutex
	cond     *sync.Cond
	queue    []*writeUnit
	batch    []*writeUnit
	errs     []error
	queueLen int
	head     int
	running  bool
	maxQ     int

	stats writeStats
}

type writeStats struct {
	Enabled bool

	Submitted       atomic.Uint64
	Enqueued        atomic.Uint64
	Dequeued        atomic.Uint64
	ExecutedBatches atomic.Uint64
	MultiBatches    atomic.Uint64
	MultiOps        atomic.Uint64
	BatchSize1      atomic.Uint64
	BatchSize2To4   atomic.Uint64
	BatchSize5To8   atomic.Uint64
	BatchSize9Plus  atomic.Uint64
	MaxBatchSeen    atomic.Uint64
	QueueHighWater  atomic.Uint64

	RejectedClosed atomic.Uint64
	TxBeginErrors  atomic.Uint64
	TxOpErrors     atomic.Uint64
	TxCommitErrors atomic.Uint64
}

type collectionFrameAttempt struct {
	collection *collection
	attempt    wexec.CollectionWriteAttempt
}

type collectionAppliedBatch struct {
	collection *collection
	applied    wexec.AppliedBatch
}

var (
	stagedCollectionWriteSlicePool  = pooled.Slices[stagedCollectionWrite]{MaxCap: 1024, Clear: pooled.ClearCap}
	collectionFrameAttemptSlicePool = pooled.Slices[collectionFrameAttempt]{MaxCap: 256, Clear: pooled.ClearCap}
	rootAppliedBatchSlicePool       = pooled.Slices[collectionAppliedBatch]{MaxCap: 1024, Clear: pooled.ClearCap}
)

type rootSchedulerSnapshot struct {
	QueueLen      int
	QueueCap      int
	WorkerRunning bool

	Submitted       uint64
	Enqueued        uint64
	Dequeued        uint64
	QueueHighWater  uint64
	ExecutedBatches uint64

	MultiUnitBatches uint64
	MultiUnitOps     uint64
	BatchSize1       uint64
	BatchSize2To4    uint64
	BatchSize5To8    uint64
	BatchSize9Plus   uint64
	AvgBatchSize     float64
	MaxBatchSeen     uint64

	RejectedClosed uint64
	TxBeginErrors  uint64
	TxOpErrors     uint64
	TxCommitErrors uint64
}

func newRootScheduler(statsEnabled bool) *writeScheduler {
	s := &writeScheduler{
		queue: make([]*writeUnit, 64),
		stats: writeStats{Enabled: statsEnabled},
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *writeScheduler) configure(maxOps int) {
	if maxOps <= 1 {
		maxOps = 1
	}
	maxQ := max(maxOps*8, 256)

	s.mu.Lock()
	if s.maxQ < maxQ {
		s.maxQ = maxQ
	}
	s.mu.Unlock()
}

func (s *writeScheduler) submit(unit *writeUnit) error {
	stats := s.stats.Enabled
	if stats {
		s.stats.Submitted.Add(1)
	}
	if unit.root.broken.Load() {
		unit.cancel()
		if stats {
			s.stats.RejectedClosed.Add(1)
		}
		return rbierrors.ErrBroken
	}

	s.mu.Lock()
	for s.maxQ > 0 && s.queueLen >= s.maxQ {
		if unit.root.broken.Load() {
			s.mu.Unlock()
			unit.cancel()
			if stats {
				s.stats.RejectedClosed.Add(1)
			}
			return rbierrors.ErrBroken
		}
		s.cond.Wait()
	}
	if !s.running && s.queueLen == 0 {
		s.running = true
		if unit.truncate != nil || unit.limit <= 1 || unit.work >= unit.limit {
			if stats {
				s.stats.Enqueued.Add(1)
				s.stats.Dequeued.Add(1)
				rootAtomicSetMax(&s.stats.QueueHighWater, 1)
			}
			s.mu.Unlock()

			err := unit.root.executeLogicalUnit(unit)
			s.stats.recordExecuted(1)
			if s.finishSubmitterRun() {
				go unit.root.runWriteScheduler()
			}
			return err
		}
		s.enqueue(unit)
		if stats {
			s.stats.Enqueued.Add(1)
			rootAtomicSetMax(&s.stats.QueueHighWater, uint64(s.queueLen))
		}
		s.mu.Unlock()

		batch := unit.root.scheduler.popBatch()
		err := unit.root.finishSubmitterBatch(batch)
		s.stats.recordExecuted(len(batch))
		clear(batch)
		if s.finishSubmitterRun() {
			go unit.root.runWriteScheduler()
		}
		return err
	}

	if unit.done == nil {
		unit.done = make(chan error, 1)
	}
	s.enqueue(unit)
	if stats {
		s.stats.Enqueued.Add(1)
		rootAtomicSetMax(&s.stats.QueueHighWater, uint64(s.queueLen))
	}
	s.mu.Unlock()

	return <-unit.done
}

func (s *writeScheduler) finishSubmitterRun() bool {
	s.mu.Lock()
	if s.queueLen == 0 {
		s.running = false
		s.cond.Broadcast()
		s.mu.Unlock()
		return false
	}
	s.mu.Unlock()
	return true
}

func (s *writeScheduler) enqueue(unit *writeUnit) {
	if s.queueLen == len(s.queue) {
		next := len(s.queue) * 2
		if next == 0 {
			next = 64
		}
		if s.maxQ > 0 && next > s.maxQ {
			next = s.maxQ
		}
		if next < s.queueLen+1 {
			next = s.queueLen + 1
		}
		queue := make([]*writeUnit, next)
		for i := 0; i < s.queueLen; i++ {
			queue[i] = s.queue[s.queueIndex(i)]
		}
		s.queue = queue
		s.head = 0
	}
	idx := s.head + s.queueLen
	if idx >= len(s.queue) {
		idx -= len(s.queue)
	}
	s.queue[idx] = unit
	s.queueLen++
}

func (s *writeScheduler) popBatch() []*writeUnit {
	s.mu.Lock()
	if s.queueLen == 0 {
		s.running = false
		s.cond.Broadcast()
		s.mu.Unlock()
		return nil
	}

	n := 1
	first := s.queue[s.head]
	if first.truncate == nil && first.work <= first.limit {
		limit := first.limit
		work := first.work
		for n < s.queueLen {
			next := s.queueAt(n)
			if next.truncate != nil {
				break
			}
			candidateLimit := min(limit, next.limit)
			candidateWork := work + next.work
			if candidateWork > candidateLimit {
				break
			}
			n++
			limit = candidateLimit
			work = candidateWork
		}
	}

	batch := s.dequeue(n)

	if s.stats.Enabled {
		s.stats.Dequeued.Add(uint64(len(batch)))
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	return batch
}

func (s *writeScheduler) queueIndex(i int) int {
	idx := s.head + i
	if idx >= len(s.queue) {
		idx -= len(s.queue)
	}
	return idx
}

func (s *writeScheduler) queueAt(i int) *writeUnit {
	return s.queue[s.queueIndex(i)]
}

func (s *writeScheduler) dequeue(n int) []*writeUnit {
	if cap(s.batch) < n {
		s.batch = make([]*writeUnit, n)
	}
	batch := s.batch[:n]
	for i := 0; i < n; i++ {
		idx := s.queueIndex(i)
		batch[i] = s.queue[idx]
		s.queue[idx] = nil
	}
	s.head += n
	if s.head >= len(s.queue) {
		s.head -= len(s.queue)
	}
	s.queueLen -= n
	if s.queueLen == 0 {
		s.head = 0
	}
	return batch
}

func (s *writeScheduler) wakeWaiters() {
	s.mu.Lock()
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *writeScheduler) snapshot() rootSchedulerSnapshot {
	if !s.stats.Enabled {
		return rootSchedulerSnapshot{}
	}

	s.mu.Lock()
	queueLen := s.queueLen
	queueCap := cap(s.queue)
	running := s.running
	s.mu.Unlock()

	out := rootSchedulerSnapshot{
		QueueLen:         queueLen,
		QueueCap:         queueCap,
		WorkerRunning:    running,
		Submitted:        s.stats.Submitted.Load(),
		Enqueued:         s.stats.Enqueued.Load(),
		Dequeued:         s.stats.Dequeued.Load(),
		QueueHighWater:   s.stats.QueueHighWater.Load(),
		ExecutedBatches:  s.stats.ExecutedBatches.Load(),
		MultiUnitBatches: s.stats.MultiBatches.Load(),
		MultiUnitOps:     s.stats.MultiOps.Load(),
		BatchSize1:       s.stats.BatchSize1.Load(),
		BatchSize2To4:    s.stats.BatchSize2To4.Load(),
		BatchSize5To8:    s.stats.BatchSize5To8.Load(),
		BatchSize9Plus:   s.stats.BatchSize9Plus.Load(),
		MaxBatchSeen:     s.stats.MaxBatchSeen.Load(),
		RejectedClosed:   s.stats.RejectedClosed.Load(),
		TxBeginErrors:    s.stats.TxBeginErrors.Load(),
		TxOpErrors:       s.stats.TxOpErrors.Load(),
		TxCommitErrors:   s.stats.TxCommitErrors.Load(),
	}
	if out.ExecutedBatches != 0 {
		out.AvgBatchSize = float64(out.Dequeued) / float64(out.ExecutedBatches)
	}
	return out
}

func (s *writeStats) recordExecuted(size int) {
	if !s.Enabled {
		return
	}
	s.ExecutedBatches.Add(1)
	if size > 1 {
		s.MultiBatches.Add(1)
		s.MultiOps.Add(uint64(size))
	}
	switch {
	case size <= 1:
		s.BatchSize1.Add(1)
	case size <= 4:
		s.BatchSize2To4.Add(1)
	case size <= 8:
		s.BatchSize5To8.Add(1)
	default:
		s.BatchSize9Plus.Add(1)
	}
	rootAtomicSetMax(&s.MaxBatchSeen, uint64(size))
}

func rootAtomicSetMax(dst *atomic.Uint64, v uint64) {
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

func (unit *writeUnit) cancel() {
	for i := len(unit.segments) - 1; i >= 0; i-- {
		unit.segments[i].ops.Cancel()
	}
	clear(unit.segments)
	unit.segments = unit.segments[:0]
	unit.releaseGeneratedCollections()
}

func (unit *writeUnit) releaseGeneratedCollections() {
	for i := len(unit.generatedCollections) - 1; i >= 0; i-- {
		unit.generatedCollections[i].releaseRetain()
	}
	clear(unit.generatedCollections)
	unit.generatedCollections = unit.generatedCollections[:0]
	if unit.hookTx != nil {
		*unit.hookTx = Tx{}
	}
}

func (r *rootStore) runWriteScheduler() {
	for {
		batch := r.scheduler.popBatch()
		if len(batch) == 0 {
			return
		}
		r.finishLogicalBatch(batch)
		r.scheduler.stats.recordExecuted(len(batch))
		clear(batch)
	}
}

func (r *rootStore) finishLogicalBatch(units []*writeUnit) {
	if len(units) == 1 {
		unit := units[0]
		unit.done <- r.executeLogicalUnit(unit)
		return
	}

	errs := r.scheduler.errs
	if cap(errs) < len(units) {
		errs = make([]error, len(units))
		r.scheduler.errs = errs
	}
	errs = errs[:len(units)]
	clear(errs)

	fatal := r.executeRootAttempt(units, errs)
	for i := range units {
		if fatal != nil && errs[i] == nil {
			errs[i] = fatal
		}
		units[i].done <- errs[i]
	}
}

func (r *rootStore) finishSubmitterBatch(units []*writeUnit) error {
	if len(units) == 1 {
		return r.executeLogicalUnit(units[0])
	}

	errs := r.scheduler.errs
	if cap(errs) < len(units) {
		errs = make([]error, len(units))
		r.scheduler.errs = errs
	}
	errs = errs[:len(units)]
	clear(errs)

	fatal := r.executeRootAttempt(units, errs)
	if fatal != nil && errs[0] == nil {
		errs[0] = fatal
	}
	retErr := errs[0]
	for i := 1; i < len(units); i++ {
		if fatal != nil && errs[i] == nil {
			errs[i] = fatal
		}
		units[i].done <- errs[i]
	}
	return retErr
}

func (r *rootStore) executeRootAttempt(units []*writeUnit, errs []error) (retErr error) {
	if r.broken.Load() {
		for i := range units {
			units[i].cancel()
		}
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.RejectedClosed.Add(uint64(len(units)))
		}
		releaseLogicalUnitGeneratedCollections(units)
		return rbierrors.ErrBroken
	}

	tx, err := r.bolt.Begin(true)
	if err != nil {
		for i := range units {
			units[i].cancel()
		}
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.TxBeginErrors.Add(1)
		}
		releaseLogicalUnitGeneratedCollections(units)
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	if r.broken.Load() {
		for i := range units {
			units[i].cancel()
		}
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.RejectedClosed.Add(uint64(len(units)))
		}
		releaseLogicalUnitGeneratedCollections(units)
		return rbierrors.ErrBroken
	}

	retErr = r.executePreparedRootFrame(tx, units, errs)
	releaseLogicalUnitGeneratedCollections(units)
	return retErr
}

func (r *rootStore) executePreparedRootFrame(tx *bbolt.Tx, units []*writeUnit, errs []error) (retErr error) {
	attempts := collectionFrameAttemptSlicePool.Get(len(units))
	touched := pooled.GetIntSlice(0)

	var appliedBatches []collectionAppliedBatch
	var err error

	for unitIdx := range units {

		unit := units[unitIdx]
		touched = touched[:0]
		failed := false

		for segmentIdx := 0; segmentIdx < len(unit.segments); segmentIdx++ {
			segment := &unit.segments[segmentIdx]
			pos := -1
			for j := range attempts {
				if attempts[j].collection == segment.collection {
					pos = j
					break
				}
			}
			if pos < 0 {
				attempt, err := segment.collection.executor.NewFrameAttempt(tx, segment.work)
				if err != nil {
					if errs != nil {
						errs[unitIdx] = err
						cancelLogicalUnits(units)
					} else {
						unit.cancel()
					}
					retErr = err
					goto CLEANUP
				}
				attempts = append(attempts, collectionFrameAttempt{
					collection: segment.collection,
					attempt:    attempt,
				})
				pos = len(attempts) - 1
			}

			found := false
			for j := range touched {
				if touched[j] == pos {
					found = true
					break
				}
			}
			if !found {
				touched = append(touched, pos)
			}

			clc := segment.collection
			ops := segment.ops

			// Prepare owns request release after this transfer; the segment is
			// cleared so later cancel paths cannot release the same batch twice.

			segment.ops = wexec.Batch{}
			var hookInsert *int
			var hookTxp unsafe.Pointer
			if ops.HasOnChange() {
				hookTx := unit.hookTx
				if hookTx == nil {
					hookTx = &Tx{}
					unit.hookTx = hookTx
				}
				*hookTx = Tx{
					root:       r,
					kind:       txKindWrite,
					state:      writeTxHook,
					hookUnit:   unit,
					hookBatch:  segmentIdx,
					hookInsert: segmentIdx + 1,
				}
				hookInsert = &hookTx.hookInsert
				hookTxp = unsafe.Pointer(hookTx)
			}

			tailWork, prepareErr := attempts[pos].attempt.Prepare(&ops, hookTxp, hookInsert)
			if prepareErr != nil {
				discardRootFrameCurrents(attempts, touched)
				if rootFrameUnitFatal(units, unitIdx, errs, prepareErr) {
					retErr = prepareErr
					goto CLEANUP
				}
				failed = true
				break
			}
			if tailWork != 0 {
				insert := *hookInsert
				// Generated writes run before the unprepared tail that followed
				// the hook-triggering request in the original batch.
				unit.segments = append(unit.segments, writeSegment{})
				copy(unit.segments[insert+1:], unit.segments[insert:])
				unit.segments[insert] = writeSegment{
					collection: clc,
					ops:        ops,
					work:       tailWork,
				}
			}
		}
		if failed {
			continue
		}

		for j := range touched {
			if err = attempts[touched[j]].attempt.ValidateCurrent(); err != nil {
				discardRootFrameCurrents(attempts, touched)
				if rootFrameUnitFatal(units, unitIdx, errs, err) {
					retErr = err
					goto CLEANUP
				}
				failed = true
				break
			}
		}
		if failed {
			continue
		}
		for j := range touched {
			attempts[touched[j]].attempt.AcceptValidatedCurrent()
		}
	}

	appliedBatches = rootAppliedBatchSlicePool.Get(len(attempts))

	for i := range attempts {
		next, err := attempts[i].attempt.Apply()
		if err != nil {
			releaseRootAppliedBatches(appliedBatches, true)
			cancelLogicalUnits(units)
			retErr = err
			goto CLEANUP
		}
		if next.Seq != 0 {
			appliedBatches = append(appliedBatches, collectionAppliedBatch{
				collection: attempts[i].collection,
				applied:    next,
			})
		}
	}
	if err = r.commitAppliedCollectionWrites(tx, appliedBatches); err != nil {
		retErr = err
	}

CLEANUP:
	if appliedBatches != nil {
		rootAppliedBatchSlicePool.Put(appliedBatches)
	}
	pooled.ReleaseIntSlice(touched)
	releaseRootFrameAttempts(attempts)
	collectionFrameAttemptSlicePool.Put(attempts)
	return retErr
}

func rootFrameUnitFatal(units []*writeUnit, unitIdx int, errs []error, err error) bool {
	units[unitIdx].cancel()
	if errs != nil && !rootWriteBatchWideErr(err) {
		errs[unitIdx] = err
		return false
	}
	if errs != nil {
		cancelLogicalUnits(units)
	}
	return true
}

func releaseLogicalUnitGeneratedCollections(units []*writeUnit) {
	for i := range units {
		units[i].releaseGeneratedCollections()
	}
}

func cancelLogicalUnits(units []*writeUnit) {
	for i := range units {
		units[i].cancel()
	}
}

func discardRootFrameCurrents(attempts []collectionFrameAttempt, touched []int) {
	for i := len(touched) - 1; i >= 0; i-- {
		attempts[touched[i]].attempt.DiscardCurrent()
	}
}

func releaseRootFrameAttempts(attempts []collectionFrameAttempt) {
	for i := range attempts {
		attempts[i].attempt.Cancel()
	}
}

func releaseRootAppliedBatches(applied []collectionAppliedBatch, releaseSnapshots bool) {
	// Snapshots are released here only before they enter a staged generation;
	// after staging, the generation/read-state lifecycle owns them.
	for i := range applied {
		if releaseSnapshots && applied[i].applied.Snapshot != nil {
			applied[i].applied.Snapshot.Release()
		}
		applied[i].applied.Release()
	}
}

func rootWriteBatchWideErr(err error) bool {
	return errors.Is(err, wexec.ErrBucketMissing) ||
		errors.Is(err, wexec.ErrStringMapBucketMissing) ||
		errors.Is(err, wexec.ErrAdvanceBucketSequence) ||
		errors.Is(err, wexec.ErrAdvanceStringMapSequence) ||
		errors.Is(err, berrors.ErrDatabaseNotOpen)
}

func (r *rootStore) commitAppliedCollectionWrites(tx *bbolt.Tx, applied []collectionAppliedBatch) error {
	if len(applied) == 0 {
		return nil
	}

	var one [1]stagedCollectionWrite
	var writes []stagedCollectionWrite
	if len(applied) == 1 {
		writes = one[:0]
	} else {
		writes = stagedCollectionWriteSlicePool.Get(len(applied))
		defer stagedCollectionWriteSlicePool.Put(writes)
	}
	for i := range applied {
		write := &applied[i]
		writes = append(writes, stagedCollectionWrite{
			collection: write.collection,
			dataSeq:    write.applied.Seq,
			snap:       write.applied.Snapshot,
		})
	}

	r.generationMu.Lock()
	rootSeq, err := r.advanceEpoch(tx)
	if err != nil {
		r.generationMu.Unlock()
		releaseRootAppliedBatches(applied, true)
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.TxOpErrors.Add(1)
		}
		return fmt.Errorf("advance root sequence: %w", err)
	}

	// stageCollectionWrites transfers snapshot ownership into the staged root
	// generation. Commit failure must drop that generation instead of releasing
	// snapshots through the AppliedBatch cleanup path.
	r.stageCollectionWrites(rootSeq, writes)
	if err = tx.Commit(); err != nil {
		r.registry.dropStaged(rootSeq)
		r.generationMu.Unlock()
		releaseRootAppliedBatches(applied, false)
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.TxCommitErrors.Add(1)
		}
		return fmt.Errorf("commit error: %w", err)
	}
	if err = r.publishWrite(rootSeq); err != nil {
		r.generationMu.Unlock()
		releaseRootAppliedBatches(applied, false)
		return err
	}
	r.generationMu.Unlock()

	for i := range applied {
		write := &applied[i]
		if write.applied.Snapshot != nil {
			if err = r.installCommitted(write.collection, write.applied.Snapshot); err != nil {
				releaseRootAppliedBatches(applied, false)
				return err
			}
		}
	}
	releaseRootAppliedBatches(applied, false)
	return nil
}

func (r *rootStore) executeLogicalUnit(unit *writeUnit) error {
	defer unit.releaseGeneratedCollections()
	if r.broken.Load() {
		unit.cancel()
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.RejectedClosed.Add(1)
		}
		return rbierrors.ErrBroken
	}
	if unit.truncate != nil {
		return r.executeTruncate(unit.truncate)
	}

	tx, err := r.bolt.Begin(true)
	if err != nil {
		unit.cancel()
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.TxBeginErrors.Add(1)
		}
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	segments := unit.segments

	if r.broken.Load() {
		unit.cancel()
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.RejectedClosed.Add(1)
		}
		return rbierrors.ErrBroken
	}

	if len(segments) == 1 {
		segment := &segments[0]
		if segment.ops.HasOnChange() {
			return r.executePreparedLogicalUnit(tx, unit)
		}
		applied, err := segment.ops.ApplyAtomic(tx)
		if err != nil {
			return err
		}
		if applied.Seq == 0 {
			return nil
		}

		appliedBatch := [1]collectionAppliedBatch{{
			collection: segment.collection,
			applied:    applied,
		}}
		return r.commitAppliedCollectionWrites(tx, appliedBatch[:])
	}

	for i := range segments {
		if segments[i].ops.HasOnChange() {
			return r.executePreparedLogicalUnit(tx, unit)
		}
	}
	for i := range segments {
		for j := 0; j < i; j++ {
			if segments[j].collection == segments[i].collection {
				return r.executePreparedLogicalUnit(tx, unit)
			}
		}
	}

	appliedBatches := rootAppliedBatchSlicePool.Get(len(segments))
	defer rootAppliedBatchSlicePool.Put(appliedBatches)

	for i := range segments {
		segment := &segments[i]
		applied, err := segment.ops.ApplyAtomic(tx)
		if err != nil {
			releaseRootAppliedBatches(appliedBatches, true)
			unit.cancel()
			return err
		}
		if applied.Seq != 0 {
			appliedBatches = append(appliedBatches, collectionAppliedBatch{
				collection: segment.collection,
				applied:    applied,
			})
		}
	}
	if len(appliedBatches) == 0 {
		return nil
	}
	return r.commitAppliedCollectionWrites(tx, appliedBatches)
}

func (r *rootStore) executePreparedLogicalUnit(tx *bbolt.Tx, unit *writeUnit) (retErr error) {
	var units [1]*writeUnit
	units[0] = unit
	return r.executePreparedRootFrame(tx, units[:], nil)
}

func (r *rootStore) executeTruncate(p *collection) error {
	tx, err := r.bolt.Begin(true)
	if err != nil {
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.TxBeginErrors.Add(1)
		}
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	if r.broken.Load() {
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.RejectedClosed.Add(1)
		}
		return rbierrors.ErrBroken
	}

	prevSeq := uint64(0)
	if bucket := tx.Bucket(p.dataBucket); bucket != nil {
		prevSeq = bucket.Sequence()
		if err = tx.DeleteBucket(p.dataBucket); err != nil {
			if r.scheduler.stats.Enabled {
				r.scheduler.stats.TxOpErrors.Add(1)
			}
			return fmt.Errorf("error deleting bucket: %w", err)
		}
	}
	if p.strKey && tx.Bucket(p.strmapBucket) != nil {
		if err = tx.DeleteBucket(p.strmapBucket); err != nil {
			if r.scheduler.stats.Enabled {
				r.scheduler.stats.TxOpErrors.Add(1)
			}
			return fmt.Errorf("error deleting string map bucket: %w", err)
		}
	}

	bucket, err := tx.CreateBucketIfNotExists(p.dataBucket)
	if err != nil {
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.TxOpErrors.Add(1)
		}
		return fmt.Errorf("error creating bucket: %w", err)
	}
	if err = bucket.SetSequence(prevSeq); err != nil {
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.TxOpErrors.Add(1)
		}
		return fmt.Errorf("restore bucket sequence: %w", err)
	}
	if p.strKey {
		if _, err = createStrMapBucket(tx, p.dataBucket); err != nil {
			if r.scheduler.stats.Enabled {
				r.scheduler.stats.TxOpErrors.Add(1)
			}
			return err
		}
	}
	seq, err := bucket.NextSequence()
	if err != nil {
		if r.scheduler.stats.Enabled {
			r.scheduler.stats.TxOpErrors.Add(1)
		}
		return fmt.Errorf("advance bucket sequence: %w", err)
	}

	var staged engine.StagedSnapshot
	stagedOK := false
	if p.index != nil {
		staged = p.index.StageTruncate(seq)
		stagedOK = true
	}
	var snap *snapshot.View
	if stagedOK {
		snap = staged.View()
	}
	applied := [1]collectionAppliedBatch{{
		collection: p,
		applied: wexec.AppliedBatch{
			Seq:      seq,
			Snapshot: snap,
		},
	}}
	return r.commitAppliedCollectionWrites(tx, applied[:])
}
