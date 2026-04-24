package rbi

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
	"go.etcd.io/bbolt"
)

/*

Auto-batcher execution model:

- Public Set/Patch/Delete and explicit Batch* writes are first translated into
  autoBatchRequest objects by build*AutoBatchRequest.
- Single-request jobs may be merged into one shared write transaction when
  repeated IDs are compatible. Set/Delete jobs without hooks may also be
  coalesced in queue order; superseded requests point to the terminal request
  via replacedBy and inherit its final error.
- Isolated jobs and explicit multi-request jobs run as atomic batches. Shared
  jobs run in a retry loop that can drop one failed request and commit the rest.

Request / job / attempt ownership:

- autoBatchRequest is the user-operation carrier. Builder phase initializes its
  immutable input state and eager queue policy. Execution only mutates err and
  replacedBy.
- req.done is the completion surface for direct executeAutoBatch paths.
- job.done is the completion surface for queued submitAutoBatchRequests paths.
- autoBatchAttemptState is per-transaction scratch. It owns prepared/accepted
  ops, per-ID runtime state, unique-check scratch, and any transient encoded
  payloads produced inside the attempt.
- autoBatchState caches the current per-ID value within one attempt. Its
  payload may borrow Bolt bytes or point to an attempt-owned re-encoded buffer.

Memory / hot-path rules:

- Steady-state hot paths rely on pooled request, job, request-scratch, and
  attempt-state objects. Heap growth is expected only from encode/decode work
  and capacity growth on new high-water marks.
- Pool configurations own canonical cleanup of pooled request/job/scratch/
  attempt objects before they return to the pool.
- Call sites must perform any per-use initialization explicitly after Get and
  before Put. Pools only manage storage reuse and canonical cleanup.

*/

type autoBatchOp uint8

const (
	autoBatchSet autoBatchOp = iota + 1
	autoBatchPatch
	autoBatchDelete
)

func autoBatchOpString(op autoBatchOp) string {
	switch op {
	case autoBatchSet:
		return "set"
	case autoBatchPatch:
		return "patch"
	case autoBatchDelete:
		return "delete"
	default:
		return fmt.Sprintf("unknown(%d)", op)
	}
}

type autoBatchReqPolicy uint8

const (
	autoBatchReqRepeatIDSafeShared autoBatchReqPolicy = 1 << iota
	autoBatchReqSetDeleteCoalescible
)

type autoBatchRequest[K ~string | ~uint64, V any] struct {
	op autoBatchOp
	id K

	setValue    *V
	setBaseline *V
	setPayload  *bytes.Buffer

	patch []Field

	patchIgnoreUnknown bool

	beforeProcess []beforeProcessFunc[K, V]
	beforeStore   []beforeStoreFunc[K, V]
	beforeCommit  []beforeCommitFunc[K, V]
	cloneValue    func(K, *V) *V
	policy        autoBatchReqPolicy

	replacedBy *autoBatchRequest[K, V]

	err  error
	done chan error
}

type autoBatchJob[K ~string | ~uint64, V any] struct {
	reqs       *pooled.SliceBuf[*autoBatchRequest[K, V]]
	isolated   bool
	done       chan error
	enqueuedAt int64
}

type autoBatchPrepared[K ~string | ~uint64, V any] struct {
	req *autoBatchRequest[K, V]

	key     []byte
	payload []byte
	idx     uint64
	idxNew  bool
	oldVal  *V
	newVal  *V
}

type autoBatchState[K ~string | ~uint64, V any] struct {
	key    []byte
	keyBuf [8]byte

	idx      uint64
	idxKnown bool
	idxNew   bool

	value           *V
	ownedPayload    *bytes.Buffer
	borrowedPayload []byte
}

type autoBatchAttemptState[K ~string | ~uint64, V any] struct {
	db           *DB[K, V]
	bucket       *bbolt.Bucket
	statsEnabled bool

	prepared      []autoBatchPrepared[K, V]
	accepted      []autoBatchPrepared[K, V]
	ownedPayloads []*bytes.Buffer

	stateByID    map[K]int
	stateByIDCap int
	states       []autoBatchState[K, V]

	uniqueIdxs    []uint64
	uniqueOldVals []*V
	uniqueNewVals []*V
}

func (req *autoBatchRequest[K, V]) payloadBytes() []byte {
	if req.setPayload == nil {
		return nil
	}
	return req.setPayload.Bytes()
}

func (req *autoBatchRequest[K, V]) hasPolicy(policy autoBatchReqPolicy) bool {
	return req.policy&policy != 0
}

func (req *autoBatchRequest[K, V]) canCoalesceSetDelete() bool {
	return req.replacedBy == nil && req.hasPolicy(autoBatchReqSetDeleteCoalescible)
}

func (req *autoBatchRequest[K, V]) canShareRepeatedID() bool {
	return req.replacedBy == nil && req.hasPolicy(autoBatchReqRepeatIDSafeShared)
}

func (st *autoBatchState[K, V]) payloadBytes() []byte {
	if st.ownedPayload != nil {
		return st.ownedPayload.Bytes()
	}
	return st.borrowedPayload
}

func (st *autoBatchState[K, V]) setOwnedPayload(buf *bytes.Buffer) {
	st.ownedPayload = buf
	st.borrowedPayload = nil
}

func (st *autoBatchState[K, V]) setBorrowedPayload(payload []byte) {
	st.ownedPayload = nil
	st.borrowedPayload = payload
}

func (st *autoBatchState[K, V]) clearPayload() {
	st.ownedPayload = nil
	st.borrowedPayload = nil
}

func (st *autoBatchState[K, V]) setKeyFromID(db *DB[K, V], id K) {
	if db.strkey {
		s := *(*string)(unsafe.Pointer(&id))
		st.key = unsafe.Slice(unsafe.StringData(s), len(s))
		return
	}
	binary.BigEndian.PutUint64(st.keyBuf[:], *(*uint64)(unsafe.Pointer(&id)))
	st.key = st.keyBuf[:]
}

func (st *autoBatchAttemptState[K, V]) rollbackCreated(ops []autoBatchPrepared[K, V]) {
	if !st.db.strkey {
		return
	}
	for i := range ops {
		op := ops[i]
		if !op.idxNew || op.oldVal != nil || op.newVal == nil {
			continue
		}
		st.db.rollbackCreatedStrIdx(op.req.id, op.idx)
	}
}

func (st *autoBatchAttemptState[K, V]) loadState(req *autoBatchRequest[K, V]) (*autoBatchState[K, V], error) {
	if pos, ok := st.stateByID[req.id]; ok {
		return &st.states[pos], nil
	}

	pos := len(st.states)
	st.states = append(st.states, autoBatchState[K, V]{})
	state := &st.states[pos]
	state.setKeyFromID(st.db, req.id)
	if prev := st.bucket.Get(state.key); prev != nil {
		oldVal, err := st.db.decode(prev)
		if err != nil {
			st.states = st.states[:pos]
			return nil, err
		}
		state.value = oldVal
		state.setBorrowedPayload(prev)
	}

	st.stateByID[req.id] = pos
	return state, nil
}

func (st *autoBatchAttemptState[K, V]) ensureIdx(id K, state *autoBatchState[K, V], create bool) {
	if state.idxKnown {
		return
	}
	if create {
		state.idx, state.idxNew = st.db.idxFromIDWithCreated(id)
	} else {
		state.idx = st.db.idxFromID(id)
	}
	state.idxKnown = true
}

func (ab *autoBatcher[K, V]) queueLen() int {
	return ab.queueSize
}

func (ab *autoBatcher[K, V]) queueIndex(i int) int {
	idx := ab.queueHead + i
	if idx >= len(ab.queue) {
		idx -= len(ab.queue)
	}
	return idx
}

func (ab *autoBatcher[K, V]) queueAt(i int) *autoBatchJob[K, V] {
	return ab.queue[ab.queueIndex(i)]
}

func (ab *autoBatcher[K, V]) growQueue() {
	newCap := len(ab.queue) * 2
	if newCap < 64 {
		newCap = 64
	}
	if ab.maxQ > 0 && newCap > ab.maxQ {
		newCap = ab.maxQ
	}
	if newCap < ab.queueSize+1 {
		newCap = ab.queueSize + 1
	}
	grown := make([]*autoBatchJob[K, V], newCap)
	for i := 0; i < ab.queueSize; i++ {
		grown[i] = ab.queueAt(i)
	}
	ab.queue = grown
	ab.queueHead = 0
}

func (ab *autoBatcher[K, V]) enqueue(job *autoBatchJob[K, V]) {
	if ab.queueSize == len(ab.queue) {
		ab.growQueue()
	}
	idx := ab.queueHead + ab.queueSize
	if idx >= len(ab.queue) {
		idx -= len(ab.queue)
	}
	ab.queue[idx] = job
	ab.queueSize++
}

func (ab *autoBatcher[K, V]) dequeueFrontScratch(n int) []*autoBatchJob[K, V] {
	batch := slices.Grow(ab.batchScratch, n)[:n]
	for i := 0; i < n; i++ {
		idx := ab.queueIndex(i)
		batch[i] = ab.queue[idx]
		ab.queue[idx] = nil
	}
	ab.queueHead += n
	if ab.queueHead >= len(ab.queue) {
		ab.queueHead -= len(ab.queue)
	}
	ab.queueSize -= n
	if ab.queueSize == 0 {
		ab.queueHead = 0
	}
	ab.batchScratch = nil
	return batch
}

func runBeforeStoreHooks[K ~string | ~uint64, V any](id K, oldVal, newVal *V, hooks []beforeStoreFunc[K, V]) error {
	for _, fn := range hooks {
		if err := fn(id, oldVal, newVal); err != nil {
			return err
		}
	}
	return nil
}

func runBeforeCommitHooks[K ~string | ~uint64, V any](tx *bbolt.Tx, id K, oldVal, newVal *V, hooks []beforeCommitFunc[K, V]) error {
	for _, fn := range hooks {
		if err := fn(tx, id, oldVal, newVal); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB[K, V]) rollbackCreatedStrIdxIfNeeded(op autoBatchPrepared[K, V]) {
	if !db.strkey {
		return
	}
	if !op.idxNew || op.oldVal != nil || op.newVal == nil {
		return
	}
	db.rollbackCreatedStrIdx(op.req.id, op.idx)
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

func (db *DB[K, V]) autoBatchSetRequestPolicy(beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V]) autoBatchReqPolicy {
	var policy autoBatchReqPolicy
	if len(beforeStore) == 0 && len(beforeCommit) == 0 {
		policy |= autoBatchReqSetDeleteCoalescible
	}
	return policy
}

func (db *DB[K, V]) autoBatchPatchRequestPolicy(
	patch []Field,
	beforeProcess []beforeProcessFunc[K, V],
	beforeStore []beforeStoreFunc[K, V],
) autoBatchReqPolicy {
	if !db.hasUnique {
		return autoBatchReqRepeatIDSafeShared
	}
	if len(beforeProcess) != 0 || len(beforeStore) != 0 {
		return 0
	}
	if db.patchTouchesUnique(patch) {
		return 0
	}
	return autoBatchReqRepeatIDSafeShared
}

func (db *DB[K, V]) autoBatchDeleteRequestPolicy(beforeCommit []beforeCommitFunc[K, V]) autoBatchReqPolicy {
	policy := autoBatchReqRepeatIDSafeShared
	if len(beforeCommit) == 0 {
		policy |= autoBatchReqSetDeleteCoalescible
	}
	return policy
}

func assignAutoBatchPreparedErr[K ~string | ~uint64, V any](ops []autoBatchPrepared[K, V], err error) {
	for _, op := range ops {
		if op.req.err == nil {
			op.req.err = err
		}
	}
}

func assignAutoBatchRequestErr[K ~string | ~uint64, V any](reqs *pooled.SliceBuf[*autoBatchRequest[K, V]], err error) {
	for i := 0; i < reqs.Len(); i++ {
		req := reqs.Get(i)
		if req != nil && req.err == nil {
			req.err = err
		}
	}
}

func roundedAutoBatchHint(hint int) int {
	if hint <= 8 {
		return 8
	}
	return 1 << bits.Len(uint(hint-1))
}

func collectActiveAutoBatchRequests[K ~string | ~uint64, V any](active, batch *pooled.SliceBuf[*autoBatchRequest[K, V]]) {
	active.Truncate()
	for i := 0; i < batch.Len(); i++ {
		req := batch.Get(i)
		if req.replacedBy == nil {
			active.Append(req)
		}
	}
}

func removeAutoBatchRequest[K ~string | ~uint64, V any](active *pooled.SliceBuf[*autoBatchRequest[K, V]], failed *autoBatchRequest[K, V]) bool {
	write := 0
	removed := false
	for i := 0; i < active.Len(); i++ {
		req := active.Get(i)
		if !removed && req == failed {
			removed = true
			continue
		}
		if write != i {
			active.Set(write, req)
		}
		write++
	}
	active.SetLen(write)
	return removed
}

func (db *DB[K, V]) buildSetAutoBatchRequest(id K, newVal *V, beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V], cloneValue func(K, *V) *V) (*autoBatchRequest[K, V], error) {
	if newVal == nil {
		return nil, ErrNilValue
	}

	req := db.autoBatcher.requestPool.Get()
	req.op = autoBatchSet
	req.id = id
	req.beforeStore = beforeStore
	req.beforeCommit = beforeCommit
	req.cloneValue = cloneValue
	if len(beforeStore) > 0 && cloneValue != nil {
		var err error
		if req.setBaseline, err = cloneBeforeStoreValue(id, newVal, cloneValue); err != nil {
			db.autoBatcher.requestPool.Put(req)
			return nil, err
		}
	} else {
		b := encodePool.Get()
		if err := db.encode(newVal, b); err != nil {
			encodePool.Put(b)
			db.autoBatcher.requestPool.Put(req)
			return nil, fmt.Errorf("encode: %w", err)
		}
		req.setValue = newVal
		req.setPayload = b
	}
	req.policy = db.autoBatchSetRequestPolicy(beforeStore, beforeCommit)
	return req, nil
}

func (db *DB[K, V]) buildPatchAutoBatchRequest(id K, fields []Field, ignoreUnknown bool, beforeProcess []beforeProcessFunc[K, V], beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V]) *autoBatchRequest[K, V] {
	req := db.autoBatcher.requestPool.Get()
	req.op = autoBatchPatch
	req.id = id
	if cap(req.patch) < len(fields) {
		req.patch = slices.Grow(req.patch, len(fields))
	}
	req.patch = req.patch[:len(fields)]
	copy(req.patch, fields)
	req.patchIgnoreUnknown = ignoreUnknown
	req.beforeProcess = beforeProcess
	req.beforeStore = beforeStore
	req.beforeCommit = beforeCommit
	req.policy = db.autoBatchPatchRequestPolicy(fields, beforeProcess, beforeStore)
	return req
}

func (db *DB[K, V]) buildDeleteAutoBatchRequest(id K, beforeCommit []beforeCommitFunc[K, V]) *autoBatchRequest[K, V] {
	req := db.autoBatcher.requestPool.Get()
	req.op = autoBatchDelete
	req.id = id
	req.beforeCommit = beforeCommit
	req.policy = db.autoBatchDeleteRequestPolicy(beforeCommit)
	return req
}

func (db *DB[K, V]) submitAutoBatchRequests(reqs *pooled.SliceBuf[*autoBatchRequest[K, V]], isolated bool) error {
	if reqs.Len() == 0 {
		return nil
	}

	stats := db.autoBatcher.statsEnabled
	if stats {
		db.autoBatcher.submitted.Add(1)
	}
	if err := db.unavailableErr(); err != nil {
		for i := 0; i < reqs.Len(); i++ {
			db.autoBatcher.requestPool.Put(reqs.Get(i))
		}
		if stats {
			db.autoBatcher.fallbackClosed.Add(1)
		}
		return err
	}

	job := db.autoBatcher.jobPool.Get()
	job.reqs = reqs
	job.isolated = isolated || reqs.Len() != 1

	db.autoBatcher.mu.Lock()
	for {
		if err := db.unavailableErr(); err != nil {
			db.autoBatcher.mu.Unlock()
			for i := 0; i < reqs.Len(); i++ {
				db.autoBatcher.requestPool.Put(reqs.Get(i))
			}
			db.autoBatcher.jobPool.Put(job)
			if stats {
				db.autoBatcher.fallbackClosed.Add(1)
			}
			return err
		}
		if !db.autoBatcher.running {
			db.autoBatcher.running = true
			go db.runAutoBatcherLoop()
		}
		if db.autoBatcher.maxQ <= 0 || db.autoBatcher.queueLen() < db.autoBatcher.maxQ {
			break
		}
		db.autoBatcher.cond.Wait()
	}

	db.autoBatcher.enqueue(job)
	select {
	case db.autoBatcher.waitNotify <- struct{}{}:
	default:
	}
	if stats {
		job.enqueuedAt = time.Now().UnixNano()
		db.autoBatcher.enqueued.Add(1)
		atomicSetMax(&db.autoBatcher.queueHighWater, uint64(db.autoBatcher.queueLen()))
	}
	db.autoBatcher.mu.Unlock()

	err := <-job.done
	for i := 0; i < reqs.Len(); i++ {
		db.autoBatcher.requestPool.Put(reqs.Get(i))
	}
	db.autoBatcher.jobPool.Put(job)
	return err
}

func (db *DB[K, V]) runAutoBatcherLoop() {
	for {
		batch := db.popAutoBatch()
		if len(batch) == 0 {
			return
		}
		if err := db.unavailableErr(); err != nil {
			db.failAutoBatchJobs(batch, err)
			clear(batch)
			db.autoBatcher.mu.Lock()
			if cap(batch) > cap(db.autoBatcher.batchScratch) {
				db.autoBatcher.batchScratch = batch[:0]
			} else if db.autoBatcher.batchScratch == nil {
				db.autoBatcher.batchScratch = batch[:0]
			}
			db.autoBatcher.mu.Unlock()
			continue
		}
		db.executeAutoBatchJobs(batch)
		clear(batch)
		db.autoBatcher.mu.Lock()
		if cap(batch) > cap(db.autoBatcher.batchScratch) {
			db.autoBatcher.batchScratch = batch[:0]
		} else if db.autoBatcher.batchScratch == nil {
			db.autoBatcher.batchScratch = batch[:0]
		}
		db.autoBatcher.mu.Unlock()
	}
}

func autoBatchFrontLimit[K ~string | ~uint64, V any](ab *autoBatcher[K, V], limit int) int {
	if limit <= 0 || limit > ab.queueLen() {
		limit = ab.queueLen()
	}
	if limit <= 1 {
		return limit
	}
	if ab.queueAt(0).isolated {
		return 1
	}
	for i := 1; i < limit; i++ {
		if ab.queueAt(i).isolated {
			return i
		}
	}
	return limit
}

func autoBatchRepeatedIDCompatible[K ~string | ~uint64, V any](prev, req *autoBatchRequest[K, V]) bool {
	return (req.canCoalesceSetDelete() && prev.canCoalesceSetDelete()) ||
		(req.canShareRepeatedID() && prev.canShareRepeatedID())
}

func (db *DB[K, V]) autoBatchWaitDurationLocked(frontLimit int) time.Duration {
	if db.autoBatcher.window <= 0 || db.autoBatcher.queueLen() >= db.autoBatcher.maxOps {
		return 0
	}

	now := time.Now()
	switch {

	case frontLimit > 1:
		return db.autoBatcher.window

	case db.autoBatcher.queueLen() == 1 &&
		(db.autoBatcher.hotBatchSize == 0 || db.autoBatcher.hotBatchSize >= 3) &&
		now.Before(db.autoBatcher.hotUntil):
		waitDur := db.autoBatcher.window
		if waitDur > 50*time.Microsecond {
			waitDur /= 2
		}
		return waitDur

	default:
		return 0
	}
}

func (db *DB[K, V]) autoBatchRepeatedIDLimitLocked(limit int) int {
	if limit <= 1 {
		return limit
	}

	lastByID := db.autoBatcher.repeatIDPool.Get(limit)
	for i := 0; i < limit; i++ {
		req := db.autoBatcher.queueAt(i).reqs.Get(0)

		prevIdx, seen := lastByID[req.id]
		if seen {
			prev := db.autoBatcher.queueAt(prevIdx).reqs.Get(0)
			if !autoBatchRepeatedIDCompatible(prev, req) {
				limit = i
				break
			}
		}
		lastByID[req.id] = i
	}
	db.autoBatcher.repeatIDPool.Put(lastByID)
	return limit
}

func (db *DB[K, V]) popAutoBatch() []*autoBatchJob[K, V] {
	stats := db.autoBatcher.statsEnabled

	db.autoBatcher.mu.Lock()
	if db.autoBatcher.queueLen() == 0 {
		db.autoBatcher.running = false
		db.autoBatcher.mu.Unlock()
		return nil
	}

	frontLimit := autoBatchFrontLimit(&db.autoBatcher, db.autoBatcher.maxOps)
	waitDur := db.autoBatchWaitDurationLocked(frontLimit)

	if waitDur > 0 {
		start := time.Now()

		db.autoBatcher.mu.Unlock()

		if frontLimit > 1 {
			time.Sleep(waitDur)

		} else {
			deadline := start.Add(waitDur)
			timer := db.autoBatcher.waitTimer

			for {
				remaining := time.Until(deadline)
				if remaining <= 0 {
					break
				}
				if timer == nil {
					timer = time.NewTimer(remaining)
					db.autoBatcher.waitTimer = timer
				} else {
					timer.Reset(remaining)
				}

				select {
				case <-timer.C:
				case <-db.autoBatcher.waitNotify:
				}
				timer.Stop()

				db.autoBatcher.mu.Lock()
				if db.autoBatcher.queueLen() == 0 {
					if stats {
						db.autoBatcher.coalesceWaits.Add(1)
						db.autoBatcher.coalesceWaitNanos.Add(uint64(time.Since(start)))
					}
					db.autoBatcher.running = false
					db.autoBatcher.mu.Unlock()
					return nil
				}
				if db.autoBatcher.hotBatchSize > 0 && db.autoBatcher.queueLen() >= db.autoBatcher.hotBatchSize {
					db.autoBatcher.mu.Unlock()
					break
				}
				db.autoBatcher.mu.Unlock()
			}
		}
		if stats {
			db.autoBatcher.coalesceWaits.Add(1)
			db.autoBatcher.coalesceWaitNanos.Add(uint64(time.Since(start)))
		}
		db.autoBatcher.mu.Lock()
		if db.autoBatcher.queueLen() == 0 {
			db.autoBatcher.running = false
			db.autoBatcher.mu.Unlock()
			return nil
		}
	}

	n := autoBatchFrontLimit(&db.autoBatcher, db.autoBatcher.maxOps)
	n = db.autoBatchRepeatedIDLimitLocked(n)

	batch := db.autoBatcher.dequeueFrontScratch(n)
	now := time.Now()
	if n > 1 {
		keepHot := 4 * db.autoBatcher.window
		if keepHot < 500*time.Microsecond {
			keepHot = 500 * time.Microsecond
		}
		db.autoBatcher.hotUntil = now.Add(keepHot)
		db.autoBatcher.hotBatchSize = n
	} else {
		db.autoBatcher.hotUntil = time.Time{}
		db.autoBatcher.hotBatchSize = 0
	}
	if len(batch) > 1 {
		db.markSupersededSetDeleteJobs(batch)
	}
	if stats {
		nowUnix := now.UnixNano()
		var queueWait uint64
		for _, job := range batch {
			if job.enqueuedAt == 0 || nowUnix <= job.enqueuedAt {
				continue
			}
			queueWait += uint64(nowUnix - job.enqueuedAt)
		}
		db.autoBatcher.dequeued.Add(uint64(len(batch)))
		db.autoBatcher.queueWaitNanos.Add(queueWait)
	}
	if db.autoBatcher.cond != nil {
		db.autoBatcher.cond.Broadcast()
	}
	db.autoBatcher.mu.Unlock()
	return batch
}

func (db *DB[K, V]) recordExecutedAutoBatchStats(size int) {
	if !db.autoBatcher.statsEnabled {
		return
	}
	db.autoBatcher.executedBatches.Add(1)
	if size > 1 {
		db.autoBatcher.multiReqBatches.Add(1)
		db.autoBatcher.multiReqOps.Add(uint64(size))
	}
	switch {
	case size <= 1:
		db.autoBatcher.batchSize1.Add(1)
	case size <= 4:
		db.autoBatcher.batchSize2To4.Add(1)
	case size <= 8:
		db.autoBatcher.batchSize5To8.Add(1)
	default:
		db.autoBatcher.batchSize9Plus.Add(1)
	}
	atomicSetMax(&db.autoBatcher.maxBatchSeen, uint64(size))
}

func (db *DB[K, V]) patchTouchesUnique(patch []Field) bool {
	for _, p := range patch {
		f, ok := db.patchMap[p.Name]
		if !ok || f == nil {
			continue
		}
		if f.Unique {
			return true
		}
		if f.DBName != "" {
			if indexed, ok := db.fields[f.DBName]; ok && indexed.Unique {
				return true
			}
		}
		if indexed, ok := db.fields[f.Name]; ok && indexed.Unique {
			return true
		}
	}
	return false
}

func (db *DB[K, V]) markSupersededSetDeleteJobs(batch []*autoBatchJob[K, V]) {
	if len(batch) < 2 {
		return
	}
	stats := db.autoBatcher.statsEnabled

	lastByID := db.autoBatcher.repeatIDPool.Get(len(batch))
	for i := range batch {
		req := batch[i].reqs.Get(0)
		req.replacedBy = nil

		prevIdx, seen := lastByID[req.id]
		if seen && req.canCoalesceSetDelete() {
			prev := batch[prevIdx].reqs.Get(0)
			if prev.canCoalesceSetDelete() {
				prev.replacedBy = req
				if stats {
					db.autoBatcher.coalescedSetDelete.Add(1)
				}
			}
		}
		lastByID[req.id] = i
	}
	db.autoBatcher.repeatIDPool.Put(lastByID)
}

func (db *DB[K, V]) prepareAutoBatchActive(att *autoBatchAttemptState[K, V], active *pooled.SliceBuf[*autoBatchRequest[K, V]]) {
	for i := 0; i < active.Len(); i++ {
		req := active.Get(i)
		if req.err != nil {
			continue
		}

		switch req.op {
		case autoBatchSet:
			db.prepareAutoBatchSet(att, req)
		case autoBatchPatch:
			db.prepareAutoBatchPatch(att, req)
		case autoBatchDelete:
			db.prepareAutoBatchDelete(att, req)
		}
	}
}

func (db *DB[K, V]) prepareAutoBatchSet(att *autoBatchAttemptState[K, V], req *autoBatchRequest[K, V]) {
	state, err := att.loadState(req)
	if err != nil {
		req.err = fmt.Errorf("decode: %w", err)
		return
	}

	oldVal := state.value
	newVal := req.setValue
	payload := req.payloadBytes()
	var ownedPayload *bytes.Buffer
	releaseDecodedNewVal := false
	defer func() {
		if releaseDecodedNewVal {
			db.ReleaseRecords(newVal)
		}
	}()

	if len(req.beforeStore) > 0 {
		if att.statsEnabled {
			db.autoBatcher.callbackOps.Add(1)
		}
		if req.cloneValue != nil {
			newVal, err = cloneBeforeStoreValue(req.id, req.setBaseline, req.cloneValue)
			if err != nil {
				req.err = err
				return
			}
		} else {
			newVal, err = db.decode(req.payloadBytes())
			if err != nil {
				req.err = fmt.Errorf("decode prepared value: %w", err)
				return
			}
			releaseDecodedNewVal = true
		}
		if err = runBeforeStoreHooks(req.id, oldVal, newVal, req.beforeStore); err != nil {
			req.err = err
			if att.statsEnabled {
				db.autoBatcher.callbackErrors.Add(1)
			}
			return
		}

		if err = db.validateIndexedStringValues(newVal); err != nil {
			req.err = err
			return
		}

		buf := encodePool.Get()
		if err = db.encode(newVal, buf); err != nil {
			encodePool.Put(buf)
			req.err = fmt.Errorf("encode: %w", err)
			return
		}
		ownedPayload = buf
		att.ownedPayloads = append(att.ownedPayloads, buf)
		payload = buf.Bytes()
	}

	if len(req.beforeStore) == 0 {
		if err = db.validateIndexedStringValues(newVal); err != nil {
			req.err = err
			return
		}
	}

	if !db.transparent {
		att.ensureIdx(req.id, state, true)
	}

	att.prepared = append(att.prepared, autoBatchPrepared[K, V]{
		req:     req,
		key:     state.key,
		payload: payload,
		idx:     state.idx,
		idxNew:  state.idxNew,
		oldVal:  oldVal,
		newVal:  newVal,
	})
	state.value = newVal
	releaseDecodedNewVal = false
	if ownedPayload != nil {
		state.setOwnedPayload(ownedPayload)
	} else {
		state.setBorrowedPayload(payload)
	}
}

func (db *DB[K, V]) prepareAutoBatchPatch(att *autoBatchAttemptState[K, V], req *autoBatchRequest[K, V]) {
	state, err := att.loadState(req)
	if err != nil {
		req.err = fmt.Errorf("failed to decode existing value: %w", err)
		return
	}
	if state.value == nil {
		return
	}
	if state.ownedPayload == nil && state.borrowedPayload == nil {
		req.err = fmt.Errorf("failed to re-decode value for patching: source payload is empty")
		return
	}

	oldVal := state.value
	newVal, err := db.decode(state.payloadBytes())
	if err != nil {
		req.err = fmt.Errorf("failed to re-decode value for patching: %w", err)
		return
	}
	releaseDecodedNewVal := true
	defer func() {
		if releaseDecodedNewVal {
			db.ReleaseRecords(newVal)
		}
	}()
	if err = db.applyPatch(newVal, req.patch, req.patchIgnoreUnknown); err != nil {
		req.err = fmt.Errorf("failed to apply patch: %w", err)
		return
	}
	if len(req.beforeProcess) > 0 {
		if err = runBeforeProcessHooks(req.id, newVal, req.beforeProcess); err != nil {
			req.err = err
			return
		}
	}
	if len(req.beforeStore) > 0 {
		if att.statsEnabled {
			db.autoBatcher.callbackOps.Add(1)
		}
		if err = runBeforeStoreHooks(req.id, oldVal, newVal, req.beforeStore); err != nil {
			req.err = err
			if att.statsEnabled {
				db.autoBatcher.callbackErrors.Add(1)
			}
			return
		}
	}

	if err = db.validateIndexedStringValues(newVal); err != nil {
		req.err = err
		return
	}

	buf := encodePool.Get()
	if err = db.encode(newVal, buf); err != nil {
		encodePool.Put(buf)
		req.err = fmt.Errorf("encode: %w", err)
		return
	}
	att.ownedPayloads = append(att.ownedPayloads, buf)

	if !db.transparent {
		att.ensureIdx(req.id, state, false)
	}

	att.prepared = append(att.prepared, autoBatchPrepared[K, V]{
		req:     req,
		key:     state.key,
		payload: buf.Bytes(),
		idx:     state.idx,
		idxNew:  state.idxNew,
		oldVal:  oldVal,
		newVal:  newVal,
	})
	state.value = newVal
	releaseDecodedNewVal = false
	state.setOwnedPayload(buf)
}

func (db *DB[K, V]) prepareAutoBatchDelete(att *autoBatchAttemptState[K, V], req *autoBatchRequest[K, V]) {
	state, err := att.loadState(req)
	if err != nil {
		req.err = fmt.Errorf("decode: %w", err)
		return
	}
	if state.value == nil {
		return
	}

	oldVal := state.value
	if !db.transparent {
		att.ensureIdx(req.id, state, false)
	}

	att.prepared = append(att.prepared, autoBatchPrepared[K, V]{
		req:    req,
		key:    state.key,
		idx:    state.idx,
		idxNew: state.idxNew,
		oldVal: oldVal,
		newVal: nil,
	})
	state.value = nil
	state.clearPayload()
}

func (db *DB[K, V]) filterAutoBatchAcceptedLocked(att *autoBatchAttemptState[K, V], atomicAll bool) error {
	att.accepted = att.prepared
	if !db.hasUnique {
		return nil
	}

	if atomicAll {
		att.uniqueIdxs = slices.Grow(att.uniqueIdxs[:0], len(att.prepared))[:len(att.prepared)]
		att.uniqueOldVals = slices.Grow(att.uniqueOldVals[:0], len(att.prepared))[:len(att.prepared)]
		att.uniqueNewVals = slices.Grow(att.uniqueNewVals[:0], len(att.prepared))[:len(att.prepared)]
		for i := range att.prepared {
			op := att.prepared[i]
			att.uniqueIdxs[i] = op.idx
			att.uniqueOldVals[i] = op.oldVal
			att.uniqueNewVals[i] = op.newVal
		}
		if err := db.checkUniqueOnWriteMulti(att.uniqueIdxs, att.uniqueOldVals, att.uniqueNewVals); err != nil {
			if att.statsEnabled {
				db.autoBatcher.uniqueRejected.Add(1)
			}
			for _, op := range att.prepared {
				op.req.err = err
			}
			return err
		}
		return nil
	}

	att.accepted = slices.Grow(att.accepted[:0], len(att.prepared))[:0]
	uniqueState := uniqueBatchCheckState{
		leaving: uniqueLeavingOuterPool.Get(),
		seen:    uniqueSeenOuterPool.Get(),
	}
	defer uniqueLeavingOuterPool.Put(uniqueState.leaving)
	defer uniqueSeenOuterPool.Put(uniqueState.seen)

	for _, op := range att.prepared {
		if err := db.checkUniqueBatchAppend(uniqueState, op.idx, op.oldVal, op.newVal); err != nil {
			op.req.err = err
			if att.statsEnabled {
				db.autoBatcher.uniqueRejected.Add(1)
			}
			db.rollbackCreatedStrIdxIfNeeded(op)
			continue
		}
		att.accepted = append(att.accepted, op)
	}
	return nil
}

func (db *DB[K, V]) applyAutoBatchAcceptedLocked(
	tx *bbolt.Tx,
	bucket *bbolt.Bucket,
	accepted []autoBatchPrepared[K, V],
	atomicAll bool,
) (*autoBatchRequest[K, V], error) {
	stats := db.autoBatcher.statsEnabled
	var callbackFailedReq *autoBatchRequest[K, V]

acceptedLoop:
	for _, op := range accepted {
		var err error
		switch op.req.op {
		case autoBatchSet, autoBatchPatch:
			if db.decodeFn == nil && len(op.payload) == 0 {
				err = fmt.Errorf(
					"invalid write payload op=%s id=%v idx=%d key_len=%d payload_len=0: empty msgpack payload",
					autoBatchOpString(op.req.op),
					op.req.id,
					op.idx,
					len(op.key),
				)
				op.req.err = err
				if stats {
					db.autoBatcher.txOpErrors.Add(1)
				}
				if !atomicAll {
					callbackFailedReq = op.req
					break acceptedLoop
				}
				return nil, err
			}
			err = bucket.Put(op.key, op.payload)
			if err != nil {
				err = fmt.Errorf(
					"put op=%s id=%v idx=%d key_len=%d payload_len=%d payload_prefix_hex=%s: %w",
					autoBatchOpString(op.req.op),
					op.req.id,
					op.idx,
					len(op.key),
					len(op.payload),
					formatDiagnosticBytesPrefix(op.payload, 32),
					err,
				)
			}
		case autoBatchDelete:
			err = bucket.Delete(op.key)
			if err != nil {
				err = fmt.Errorf(
					"delete op=%s id=%v idx=%d key_len=%d: %w",
					autoBatchOpString(op.req.op),
					op.req.id,
					op.idx,
					len(op.key),
					err,
				)
			}
		default:
			err = fmt.Errorf("unknown auto-batch op: %v", op.req.op)
		}
		if err != nil {
			if stats {
				db.autoBatcher.txOpErrors.Add(1)
			}
			return nil, err
		}

		if len(op.req.beforeCommit) > 0 {
			if stats && len(op.req.beforeStore) == 0 {
				db.autoBatcher.callbackOps.Add(1)
			}
			if err = runBeforeCommitHooks(tx, op.req.id, op.oldVal, op.newVal, op.req.beforeCommit); err != nil {
				err = fmt.Errorf(
					"before_commit op=%s id=%v idx=%d key_len=%d payload_len=%d: %w",
					autoBatchOpString(op.req.op),
					op.req.id,
					op.idx,
					len(op.key),
					len(op.payload),
					err,
				)
				op.req.err = err
				if stats {
					db.autoBatcher.callbackErrors.Add(1)
					db.autoBatcher.txOpErrors.Add(1)
				}
				if !atomicAll {
					callbackFailedReq = op.req
					break
				}
				return nil, err
			}
		}
	}

	if callbackFailedReq != nil {
		return callbackFailedReq, nil
	}
	return nil, nil
}

func (db *DB[K, V]) executeAutoBatchJobs(batch []*autoBatchJob[K, V]) {
	db.recordExecutedAutoBatchStats(len(batch))
	stats := db.autoBatcher.statsEnabled
	var started time.Time
	if stats {
		started = time.Now()
	}

	if len(batch) == 1 && batch[0].isolated {
		db.runAutoBatchAtomic(batch[0].reqs)
		db.finishAutoBatchJobs(batch)
		if stats {
			db.autoBatcher.executeNanos.Add(uint64(time.Since(started)))
		}
		return
	}

	reqScratch := db.autoBatcher.requestScratchPool.Get()
	reqScratch.Grow(len(batch))

	for _, job := range batch {
		if job.reqs.Len() != 1 {
			assignAutoBatchRequestErr(job.reqs, fmt.Errorf("internal auto-batch error: mixed grouped request in shared batch"))
			continue
		}
		reqScratch.Append(job.reqs.Get(0))
	}
	if reqScratch.Len() != 0 {
		db.runAutoBatchShared(reqScratch)
	}
	db.finishAutoBatchJobs(batch)
	db.autoBatcher.requestScratchPool.Put(reqScratch)
	if stats {
		db.autoBatcher.executeNanos.Add(uint64(time.Since(started)))
	}
}

func resolveAutoBatchRequestErrs[K ~string | ~uint64, V any](batch *pooled.SliceBuf[*autoBatchRequest[K, V]]) {
	for i := 0; i < batch.Len(); i++ {
		req := batch.Get(i)
		if req.replacedBy == nil {
			continue
		}
		target := req.replacedBy
		for target.replacedBy != nil {
			target = target.replacedBy
		}
		req.err = target.err
	}
}

func autoBatchJobErr[K ~string | ~uint64, V any](job *autoBatchJob[K, V]) error {
	if job.reqs.Len() == 1 {
		return job.reqs.Get(0).err
	}
	return firstAutoBatchRequestErr(job.reqs)
}

func (db *DB[K, V]) finishAutoBatchJobs(batch []*autoBatchJob[K, V]) {
	for _, job := range batch {
		resolveAutoBatchRequestErrs(job.reqs)
	}
	for _, job := range batch {
		err := autoBatchJobErr(job)
		job.done <- err
	}
}

func (db *DB[K, V]) executeAutoBatch(batch []*autoBatchRequest[K, V]) {
	db.recordExecutedAutoBatchStats(len(batch))
	stats := db.autoBatcher.statsEnabled
	var started time.Time
	if stats {
		started = time.Now()
	}
	reqScratch := db.autoBatcher.requestScratchPool.Get()
	reqScratch.AppendAll(batch)
	db.runAutoBatchShared(reqScratch)
	db.autoBatcher.requestScratchPool.Put(reqScratch)
	db.finishAutoBatch(batch)
	if stats {
		db.autoBatcher.executeNanos.Add(uint64(time.Since(started)))
	}
}

func (db *DB[K, V]) runAutoBatchShared(batch *pooled.SliceBuf[*autoBatchRequest[K, V]]) {
	for i := 0; i < batch.Len(); i++ {
		batch.Get(i).err = nil
	}

	activeScratch := db.autoBatcher.requestScratchPool.Get()
	defer db.autoBatcher.requestScratchPool.Put(activeScratch)

	activeScratch.Grow(batch.Len())

	collectActiveAutoBatchRequests(activeScratch, batch)

	for {
		retryWithoutReq, done, fatalErr := db.executeAutoBatchAttempt(activeScratch, false)
		if fatalErr != nil {
			assignAutoBatchRequestErr(batch, fatalErr)
			return
		}
		if done {
			return
		}

		if !removeAutoBatchRequest(activeScratch, retryWithoutReq) {
			assignAutoBatchRequestErr(batch, fmt.Errorf("internal auto-batch retry error: failed request not found"))
			return
		}
		if activeScratch.Len() == 0 {
			return
		}
		for i := 0; i < activeScratch.Len(); i++ {
			req := activeScratch.Get(i)
			if errors.Is(req.err, ErrUniqueViolation) {
				req.err = nil
			}
		}
	}
}

func firstAutoBatchRequestErr[K ~string | ~uint64, V any](reqs *pooled.SliceBuf[*autoBatchRequest[K, V]]) error {
	for i := 0; i < reqs.Len(); i++ {
		req := reqs.Get(i)
		if req != nil && req.err != nil {
			return req.err
		}
	}
	return nil
}

func singleAutoBatchOpName[K ~string | ~uint64, V any](req *autoBatchRequest[K, V]) string {
	if req == nil {
		return "batch"
	}
	switch req.op {
	case autoBatchSet:
		return "set"
	case autoBatchPatch:
		return "patch"
	case autoBatchDelete:
		return "delete"
	default:
		return "batch"
	}
}

func batchAutoBatchOpName[K ~string | ~uint64, V any](req *autoBatchRequest[K, V]) string {
	if req == nil {
		return "batch"
	}
	switch req.op {
	case autoBatchSet:
		return "batch_set"
	case autoBatchPatch:
		return "batch_patch"
	case autoBatchDelete:
		return "batch_delete"
	default:
		return "batch"
	}
}

func (db *DB[K, V]) autoBatchOpName(reqs *pooled.SliceBuf[*autoBatchRequest[K, V]], atomicAll bool) string {
	if reqs.Len() == 0 {
		return "batch"
	}
	if !atomicAll {
		if reqs.Len() == 1 && db.autoBatcher.maxOps <= 1 {
			return singleAutoBatchOpName(reqs.Get(0))
		}
		return "batch"
	}
	if reqs.Len() == 1 {
		return singleAutoBatchOpName(reqs.Get(0))
	}
	return batchAutoBatchOpName(reqs.Get(0))
}

func (db *DB[K, V]) runAutoBatchAtomic(batch *pooled.SliceBuf[*autoBatchRequest[K, V]]) {
	for i := 0; i < batch.Len(); i++ {
		req := batch.Get(i)
		req.err = nil
		req.replacedBy = nil
	}
	_, done, fatalErr := db.executeAutoBatchAttempt(batch, true)
	if fatalErr != nil {
		assignAutoBatchRequestErr(batch, fatalErr)
		return
	}
	if !done {
		assignAutoBatchRequestErr(batch, fmt.Errorf("internal auto-batch atomic retry error"))
	}
}

func (db *DB[K, V]) executeAutoBatchAttempt(active *pooled.SliceBuf[*autoBatchRequest[K, V]], atomicAll bool) (*autoBatchRequest[K, V], bool, error) {
	stats := db.autoBatcher.statsEnabled
	opName := db.autoBatchOpName(active, atomicAll)

	tx, err := db.bolt.Begin(true)
	if err != nil {
		if stats {
			db.autoBatcher.txBeginErrors.Add(1)
		}
		return nil, true, fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		if stats {
			db.autoBatcher.txOpErrors.Add(1)
		}
		return nil, true, fmt.Errorf("bucket does not exist")
	}

	bucket.FillPercent = db.options.BucketFillPercent

	capHint := roundedAutoBatchHint(max(active.Len(), 1))
	att := db.autoBatcher.attemptStatePool.Get()
	att.db = db
	att.bucket = bucket
	att.statsEnabled = db.autoBatcher.statsEnabled
	if cap(att.prepared) < capHint {
		att.prepared = slices.Grow(att.prepared, capHint)
	}
	if cap(att.accepted) < capHint {
		att.accepted = slices.Grow(att.accepted, capHint)
	}
	if cap(att.ownedPayloads) < capHint {
		att.ownedPayloads = slices.Grow(att.ownedPayloads, capHint)
	}
	if cap(att.states) < capHint {
		att.states = slices.Grow(att.states, capHint)
	}
	if cap(att.uniqueIdxs) < capHint {
		att.uniqueIdxs = slices.Grow(att.uniqueIdxs, capHint)
	}
	if cap(att.uniqueOldVals) < capHint {
		att.uniqueOldVals = slices.Grow(att.uniqueOldVals, capHint)
	}
	if cap(att.uniqueNewVals) < capHint {
		att.uniqueNewVals = slices.Grow(att.uniqueNewVals, capHint)
	}
	switch {
	case att.stateByID == nil:
		att.stateByID = make(map[K]int, capHint)
		att.stateByIDCap = capHint
	case capHint > att.stateByIDCap:
		att.stateByID = make(map[K]int, capHint)
		att.stateByIDCap = capHint
	}
	defer db.autoBatcher.attemptStatePool.Put(att)
	db.prepareAutoBatchActive(att, active)

	if atomicAll {
		if reqErr := firstAutoBatchRequestErr(active); reqErr != nil {
			att.rollbackCreated(att.prepared)
			return nil, true, reqErr
		}
	}

	if len(att.prepared) == 0 {
		return nil, true, nil
	}

	db.mu.Lock()
	if err = db.unavailableErr(); err != nil {
		att.rollbackCreated(att.prepared)
		db.mu.Unlock()
		assignAutoBatchPreparedErr(att.prepared, err)
		return nil, true, nil
	}

	if err = db.filterAutoBatchAcceptedLocked(att, atomicAll); err != nil {
		att.rollbackCreated(att.prepared)
		db.mu.Unlock()
		return nil, true, err
	}

	if len(att.accepted) == 0 {
		db.mu.Unlock()
		return nil, true, nil
	}

	retryWithoutReq, err := db.applyAutoBatchAcceptedLocked(tx, bucket, att.accepted, atomicAll)
	if err != nil {
		att.rollbackCreated(att.accepted)
		db.mu.Unlock()
		return nil, true, err
	}
	if retryWithoutReq != nil {
		att.rollbackCreated(att.accepted)
		db.mu.Unlock()
		return retryWithoutReq, false, nil
	}

	if db.transparent {
		if _, err = bucket.NextSequence(); err != nil {
			if stats {
				db.autoBatcher.txOpErrors.Add(1)
			}
			att.rollbackCreated(att.accepted)
			db.mu.Unlock()
			return nil, true, fmt.Errorf("advance bucket sequence: %w", err)
		}
		if err = db.commit(tx, opName); err != nil {
			if stats {
				db.autoBatcher.txCommitErrors.Add(1)
			}
			db.mu.Unlock()
			assignAutoBatchPreparedErr(att.accepted, err)
			return nil, true, nil
		}
		db.mu.Unlock()
		return nil, true, nil
	}

	seq, err := bucket.NextSequence()
	if err != nil {
		if stats {
			db.autoBatcher.txOpErrors.Add(1)
		}
		att.rollbackCreated(att.accepted)
		db.mu.Unlock()
		return nil, true, fmt.Errorf("advance bucket sequence: %w", err)
	}

	snap := db.buildPreparedSnapshotNoLock(seq, att.accepted)
	db.stageSnapshot(snap)

	if err = db.commit(tx, opName); err != nil {
		if stats {
			db.autoBatcher.txCommitErrors.Add(1)
		}
		db.dropStagedSnapshot(seq)
		att.rollbackCreated(att.accepted)
		db.mu.Unlock()
		assignAutoBatchPreparedErr(att.accepted, err)
		return nil, true, nil
	}

	err = db.publishAfterCommitLocked(seq, opName, func() {
		db.finishSnapshotPublishNoLock(snap)
	})
	if err != nil {
		db.mu.Unlock()
		assignAutoBatchPreparedErr(att.accepted, err)
		return nil, true, nil
	}
	db.mu.Unlock()

	return nil, true, nil
}

func (db *DB[K, V]) failAutoBatchJobs(batch []*autoBatchJob[K, V], err error) {
	for _, job := range batch {
		assignAutoBatchRequestErr(job.reqs, err)
	}
	db.finishAutoBatchJobs(batch)
}

func (db *DB[K, V]) finishAutoBatch(batch []*autoBatchRequest[K, V]) {
	for _, req := range batch {
		if req.replacedBy == nil {
			continue
		}
		target := req.replacedBy
		for target.replacedBy != nil {
			target = target.replacedBy
		}
		req.err = target.err
	}
	for _, req := range batch {
		if req.setPayload != nil {
			encodePool.Put(req.setPayload)
			req.setPayload = nil
		}
		req.setValue = nil
		req.setBaseline = nil
		clear(req.patch)
		req.patch = req.patch[:0]
		req.patchIgnoreUnknown = false
		req.beforeProcess = nil
		req.beforeStore = nil
		req.beforeCommit = nil
		req.cloneValue = nil
		req.policy = 0
		req.replacedBy = nil
		req.done <- req.err
	}
}
