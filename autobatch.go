package rbi

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

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
- Request-owned payloads/hooks are released exactly once in releaseOwnedState
  before the request returns to the pool.
- Attempt-owned buffers are tracked in ownedPayloads and released in
  releaseAttemptState after commit/rollback.
- acquire* functions assume release* restored canonical pooled state. They do
  not repeat defensive normalization of already-owned invariants.

*/

type autoBatchOp uint8

const (
	autoBatchSet autoBatchOp = iota + 1
	autoBatchPatch
	autoBatchDelete
)

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
	reqs     []*autoBatchRequest[K, V]
	isolated bool
	done     chan error
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
	key []byte

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

type autoBatchRequestScratch[K ~string | ~uint64, V any] struct {
	reqs []*autoBatchRequest[K, V]
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

func (req *autoBatchRequest[K, V]) releaseOwnedState() {
	if req.setPayload != nil {
		releaseEncodeBuf(req.setPayload)
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
}

func releaseAutoBatchRequestsState[K ~string | ~uint64, V any](reqs []*autoBatchRequest[K, V]) {
	for i := range reqs {
		reqs[i].releaseOwnedState()
	}
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

	state := autoBatchState[K, V]{
		key: st.db.keyFromID(req.id),
	}
	if prev := st.bucket.Get(state.key); prev != nil {
		oldVal, err := st.db.decode(prev)
		if err != nil {
			return nil, err
		}
		state.value = oldVal
		state.setBorrowedPayload(prev)
	}

	pos := len(st.states)
	st.states = append(st.states, state)
	st.stateByID[req.id] = pos
	return &st.states[pos], nil
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

func (ab *autoBatcher[K, V]) releaseBatchScratch(batch []*autoBatchJob[K, V]) {
	clear(batch)
	ab.mu.Lock()
	if cap(batch) > cap(ab.batchScratch) {
		ab.batchScratch = batch[:0]
	} else if ab.batchScratch == nil {
		ab.batchScratch = batch[:0]
	}
	ab.mu.Unlock()
}

func (ab *autoBatcher[K, V]) acquireRepeatIDMap(hint int) map[K]int {
	if hint < 8 {
		hint = 8
	}
	switch {
	case ab.repeatIDScratch == nil:
		ab.repeatIDScratch = make(map[K]int, hint)
		ab.repeatIDScratchCap = hint
	case hint > ab.repeatIDScratchCap:
		ab.repeatIDScratch = make(map[K]int, hint)
		ab.repeatIDScratchCap = hint
	default:
	}
	return ab.repeatIDScratch
}

func (ab *autoBatcher[K, V]) releaseRepeatIDMap() {
	if ab.repeatIDScratch != nil {
		clear(ab.repeatIDScratch)
	}
}

func (ab *autoBatcher[K, V]) acquireRequestScratch(hint int) *autoBatchRequestScratch[K, V] {
	if hint < 1 {
		hint = 1
	}

	var scratch *autoBatchRequestScratch[K, V]
	if v := ab.requestScratchPool.Get(); v != nil {
		scratch = v.(*autoBatchRequestScratch[K, V])
	} else {
		scratch = &autoBatchRequestScratch[K, V]{}
	}
	if cap(scratch.reqs) < hint {
		scratch.reqs = slices.Grow(scratch.reqs, hint)
	}
	return scratch
}

func (ab *autoBatcher[K, V]) releaseRequestScratch(scratch *autoBatchRequestScratch[K, V]) {
	clear(scratch.reqs)
	scratch.reqs = scratch.reqs[:0]
	ab.requestScratchPool.Put(scratch)
}

func (ab *autoBatcher[K, V]) acquireRequest(op autoBatchOp, id K) *autoBatchRequest[K, V] {
	var req *autoBatchRequest[K, V]
	if v := ab.requestPool.Get(); v != nil {
		req = v.(*autoBatchRequest[K, V])
	} else {
		req = &autoBatchRequest[K, V]{}
	}
	if req.done == nil {
		req.done = make(chan error, 1)
	}
	req.op = op
	req.id = id
	return req
}

func (ab *autoBatcher[K, V]) releaseRequest(req *autoBatchRequest[K, V]) {
	select {
	case <-req.done:
	default:
	}
	req.op = 0
	var zeroID K
	req.id = zeroID
	req.err = nil
	ab.requestPool.Put(req)
}

func (ab *autoBatcher[K, V]) releaseRequests(reqs []*autoBatchRequest[K, V]) {
	for i := range reqs {
		ab.releaseRequest(reqs[i])
	}
}

func (ab *autoBatcher[K, V]) acquireJob(reqs []*autoBatchRequest[K, V], isolated bool) *autoBatchJob[K, V] {
	var job *autoBatchJob[K, V]
	if v := ab.jobPool.Get(); v != nil {
		job = v.(*autoBatchJob[K, V])
	} else {
		job = &autoBatchJob[K, V]{}
	}
	if job.done == nil {
		job.done = make(chan error, 1)
	}
	job.reqs = reqs
	job.isolated = isolated
	return job
}

func (ab *autoBatcher[K, V]) releaseJob(job *autoBatchJob[K, V]) {
	select {
	case <-job.done:
	default:
	}
	job.reqs = nil
	job.isolated = false
	ab.jobPool.Put(job)
}

func (ab *autoBatcher[K, V]) acquireAttemptState(db *DB[K, V], bucket *bbolt.Bucket, hint int) *autoBatchAttemptState[K, V] {
	if hint < 1 {
		hint = 1
	}

	var st *autoBatchAttemptState[K, V]
	if v := ab.attemptStatePool.Get(); v != nil {
		st = v.(*autoBatchAttemptState[K, V])
	} else {
		st = &autoBatchAttemptState[K, V]{}
	}

	st.db = db
	st.bucket = bucket
	st.statsEnabled = db.autoBatcher.statsEnabled

	if cap(st.prepared) < hint {
		st.prepared = slices.Grow(st.prepared, hint)
	}
	if cap(st.accepted) < hint {
		st.accepted = slices.Grow(st.accepted, hint)
	}
	if cap(st.ownedPayloads) < hint {
		st.ownedPayloads = slices.Grow(st.ownedPayloads, hint)
	}
	if cap(st.states) < hint {
		st.states = slices.Grow(st.states, hint)
	}
	if cap(st.uniqueIdxs) < hint {
		st.uniqueIdxs = slices.Grow(st.uniqueIdxs, hint)
	}
	if cap(st.uniqueOldVals) < hint {
		st.uniqueOldVals = slices.Grow(st.uniqueOldVals, hint)
	}
	if cap(st.uniqueNewVals) < hint {
		st.uniqueNewVals = slices.Grow(st.uniqueNewVals, hint)
	}

	if hint < 8 {
		hint = 8
	}
	switch {
	case st.stateByID == nil:
		st.stateByID = make(map[K]int, hint)
		st.stateByIDCap = hint
	case hint > st.stateByIDCap:
		st.stateByID = make(map[K]int, hint)
		st.stateByIDCap = hint
	}
	return st
}

func (ab *autoBatcher[K, V]) releaseAttemptState(st *autoBatchAttemptState[K, V]) {
	for _, buf := range st.ownedPayloads {
		if buf != nil {
			releaseEncodeBuf(buf)
		}
	}
	clear(st.ownedPayloads)
	st.ownedPayloads = st.ownedPayloads[:0]

	clear(st.prepared)
	st.prepared = st.prepared[:0]

	clear(st.accepted)
	st.accepted = st.accepted[:0]

	clear(st.states)
	st.states = st.states[:0]

	st.uniqueIdxs = st.uniqueIdxs[:0]

	clear(st.uniqueOldVals)
	st.uniqueOldVals = st.uniqueOldVals[:0]

	clear(st.uniqueNewVals)
	st.uniqueNewVals = st.uniqueNewVals[:0]

	if st.stateByID != nil {
		clear(st.stateByID)
	}
	st.db = nil
	st.bucket = nil
	st.statsEnabled = false

	ab.attemptStatePool.Put(st)
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

func assignAutoBatchRequestErr[K ~string | ~uint64, V any](reqs []*autoBatchRequest[K, V], err error) {
	for _, req := range reqs {
		if req != nil && req.err == nil {
			req.err = err
		}
	}
}

func collectActiveAutoBatchRequests[K ~string | ~uint64, V any](active, batch []*autoBatchRequest[K, V]) []*autoBatchRequest[K, V] {
	for _, req := range batch {
		if req.replacedBy == nil {
			active = append(active, req)
		}
	}
	return active
}

func removeAutoBatchRequest[K ~string | ~uint64, V any](active []*autoBatchRequest[K, V], failed *autoBatchRequest[K, V]) ([]*autoBatchRequest[K, V], bool) {
	out := active[:0]
	removed := false
	for _, req := range active {
		if !removed && req == failed {
			removed = true
			continue
		}
		out = append(out, req)
	}
	return out, removed
}

func (db *DB[K, V]) buildSetAutoBatchRequest(id K, newVal *V, beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V], cloneValue func(K, *V) *V) (*autoBatchRequest[K, V], error) {
	if newVal == nil {
		return nil, ErrNilValue
	}

	req := db.autoBatcher.acquireRequest(autoBatchSet, id)
	req.beforeStore = beforeStore
	req.beforeCommit = beforeCommit
	req.cloneValue = cloneValue
	if len(beforeStore) > 0 && cloneValue != nil {
		var err error
		if req.setBaseline, err = cloneBeforeStoreValue(id, newVal, cloneValue); err != nil {
			req.releaseOwnedState()
			db.autoBatcher.releaseRequest(req)
			return nil, err
		}
	} else {
		b := getEncodeBuf()
		if err := db.encode(newVal, b); err != nil {
			releaseEncodeBuf(b)
			req.releaseOwnedState()
			db.autoBatcher.releaseRequest(req)
			return nil, fmt.Errorf("encode: %w", err)
		}
		req.setValue = newVal
		req.setPayload = b
	}
	req.policy = db.autoBatchSetRequestPolicy(beforeStore, beforeCommit)
	return req, nil
}

func (db *DB[K, V]) buildPatchAutoBatchRequest(id K, fields []Field, ignoreUnknown bool, beforeProcess []beforeProcessFunc[K, V], beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V]) *autoBatchRequest[K, V] {
	req := db.autoBatcher.acquireRequest(autoBatchPatch, id)
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
	req := db.autoBatcher.acquireRequest(autoBatchDelete, id)
	req.beforeCommit = beforeCommit
	req.policy = db.autoBatchDeleteRequestPolicy(beforeCommit)
	return req
}

func (db *DB[K, V]) submitAutoBatchRequests(reqs []*autoBatchRequest[K, V], isolated bool) error {
	if len(reqs) == 0 {
		return nil
	}

	stats := db.autoBatcher.statsEnabled
	if stats {
		db.autoBatcher.submitted.Add(1)
	}
	if err := db.unavailableErr(); err != nil {
		releaseAutoBatchRequestsState(reqs)
		db.autoBatcher.releaseRequests(reqs)
		if stats {
			db.autoBatcher.fallbackClosed.Add(1)
		}
		return err
	}

	job := db.autoBatcher.acquireJob(reqs, isolated || len(reqs) != 1)

	db.autoBatcher.mu.Lock()
	for {
		if err := db.unavailableErr(); err != nil {
			db.autoBatcher.mu.Unlock()
			releaseAutoBatchRequestsState(reqs)
			db.autoBatcher.releaseRequests(reqs)
			db.autoBatcher.releaseJob(job)
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
	if stats {
		db.autoBatcher.enqueued.Add(1)
		atomicSetMax(&db.autoBatcher.queueHighWater, uint64(db.autoBatcher.queueLen()))
	}
	db.autoBatcher.mu.Unlock()

	err := <-job.done
	db.autoBatcher.releaseRequests(reqs)
	db.autoBatcher.releaseJob(job)
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
			db.autoBatcher.releaseBatchScratch(batch)
			continue
		}
		db.executeAutoBatchJobs(batch)
		db.autoBatcher.releaseBatchScratch(batch)
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
		keepHot := 4 * db.autoBatcher.window
		if keepHot < 500*time.Microsecond {
			keepHot = 500 * time.Microsecond
		}
		db.autoBatcher.hotUntil = now.Add(keepHot)
		return db.autoBatcher.window

	case db.autoBatcher.queueLen() == 1 && now.Before(db.autoBatcher.hotUntil):
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

	lastByID := db.autoBatcher.acquireRepeatIDMap(limit)
	for i := 0; i < limit; i++ {
		req := db.autoBatcher.queueAt(i).reqs[0]

		prevIdx, seen := lastByID[req.id]
		if seen {
			prev := db.autoBatcher.queueAt(prevIdx).reqs[0]
			if !autoBatchRepeatedIDCompatible(prev, req) {
				limit = i
				break
			}
		}
		lastByID[req.id] = i
	}
	db.autoBatcher.releaseRepeatIDMap()
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
		db.autoBatcher.mu.Unlock()
		start := time.Now()
		time.Sleep(waitDur)
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
	if len(batch) > 1 {
		db.markSupersededSetDeleteJobs(batch)
	}
	if stats {
		db.autoBatcher.dequeued.Add(uint64(len(batch)))
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

	lastByID := db.autoBatcher.acquireRepeatIDMap(len(batch))
	for i := range batch {
		req := batch[i].reqs[0]
		req.replacedBy = nil

		prevIdx, seen := lastByID[req.id]
		if seen && req.canCoalesceSetDelete() {
			prev := batch[prevIdx].reqs[0]
			if prev.canCoalesceSetDelete() {
				prev.replacedBy = req
				if stats {
					db.autoBatcher.coalescedSetDelete.Add(1)
				}
			}
		}
		lastByID[req.id] = i
	}
	db.autoBatcher.releaseRepeatIDMap()
}

func (db *DB[K, V]) prepareAutoBatchActive(att *autoBatchAttemptState[K, V], active []*autoBatchRequest[K, V]) {
	for _, req := range active {
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
		}
		if err = runBeforeStoreHooks(req.id, oldVal, newVal, req.beforeStore); err != nil {
			req.err = err
			if att.statsEnabled {
				db.autoBatcher.callbackErrors.Add(1)
			}
			return
		}

		buf := getEncodeBuf()
		if err = db.encode(newVal, buf); err != nil {
			releaseEncodeBuf(buf)
			req.err = fmt.Errorf("encode: %w", err)
			return
		}
		ownedPayload = buf
		att.ownedPayloads = append(att.ownedPayloads, buf)
		payload = buf.Bytes()
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

	buf := getEncodeBuf()
	if err = db.encode(newVal, buf); err != nil {
		releaseEncodeBuf(buf)
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
	uniqueState := db.newUniqueBatchCheckState()
	defer db.releaseUniqueBatchCheckState(uniqueState)

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

	for _, op := range accepted {
		var err error
		switch op.req.op {
		case autoBatchSet, autoBatchPatch:
			err = bucket.Put(op.key, op.payload)
			if err != nil {
				err = fmt.Errorf("put: %w", err)
			}
		case autoBatchDelete:
			err = bucket.Delete(op.key)
			if err != nil {
				err = fmt.Errorf("delete: %w", err)
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

		if len(op.req.beforeCommit) == 0 {
			continue
		}
		if stats && len(op.req.beforeStore) == 0 {
			db.autoBatcher.callbackOps.Add(1)
		}
		if err = runBeforeCommitHooks(tx, op.req.id, op.oldVal, op.newVal, op.req.beforeCommit); err != nil {
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

	if callbackFailedReq != nil {
		return callbackFailedReq, nil
	}
	return nil, nil
}

func (db *DB[K, V]) executeAutoBatchJobs(batch []*autoBatchJob[K, V]) {
	db.recordExecutedAutoBatchStats(len(batch))

	if len(batch) == 1 && batch[0].isolated {
		db.runAutoBatchAtomic(batch[0].reqs)
		db.finishAutoBatchJobs(batch)
		return
	}

	reqScratch := db.autoBatcher.acquireRequestScratch(len(batch))
	reqs := reqScratch.reqs
	for _, job := range batch {
		if len(job.reqs) != 1 {
			assignAutoBatchRequestErr(job.reqs, fmt.Errorf("internal auto-batch error: mixed grouped request in shared batch"))
			continue
		}
		reqs = append(reqs, job.reqs[0])
	}
	if len(reqs) != 0 {
		reqScratch.reqs = reqs
		db.runAutoBatchShared(reqs)
	}
	db.finishAutoBatchJobs(batch)
	db.autoBatcher.releaseRequestScratch(reqScratch)
}

func resolveAutoBatchRequestErrs[K ~string | ~uint64, V any](batch []*autoBatchRequest[K, V]) {
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
}

func autoBatchJobErr[K ~string | ~uint64, V any](job *autoBatchJob[K, V]) error {
	if len(job.reqs) == 1 {
		return job.reqs[0].err
	}
	return firstAutoBatchRequestErr(job.reqs)
}

func (db *DB[K, V]) finishAutoBatchJobs(batch []*autoBatchJob[K, V]) {
	for _, job := range batch {
		resolveAutoBatchRequestErrs(job.reqs)
	}
	for _, job := range batch {
		err := autoBatchJobErr(job)
		releaseAutoBatchRequestsState(job.reqs)
		job.done <- err
	}
}

func (db *DB[K, V]) executeAutoBatch(batch []*autoBatchRequest[K, V]) {
	db.recordExecutedAutoBatchStats(len(batch))
	db.runAutoBatchShared(batch)
	db.finishAutoBatch(batch)
}

func (db *DB[K, V]) runAutoBatchShared(batch []*autoBatchRequest[K, V]) {
	for _, req := range batch {
		req.err = nil
	}

	activeScratch := db.autoBatcher.acquireRequestScratch(len(batch))
	defer db.autoBatcher.releaseRequestScratch(activeScratch)

	active := collectActiveAutoBatchRequests(activeScratch.reqs, batch)
	activeScratch.reqs = active

	for {
		retryWithoutReq, done, fatalErr := db.executeAutoBatchAttempt(active, false)
		if fatalErr != nil {
			assignAutoBatchRequestErr(batch, fatalErr)
			return
		}
		if done {
			return
		}

		var removed bool
		active, removed = removeAutoBatchRequest(active, retryWithoutReq)
		activeScratch.reqs = active
		if !removed {
			assignAutoBatchRequestErr(batch, fmt.Errorf("internal auto-batch retry error: failed request not found"))
			return
		}
		if len(active) == 0 {
			return
		}
		for _, req := range active {
			if errors.Is(req.err, ErrUniqueViolation) {
				req.err = nil
			}
		}
	}
}

func firstAutoBatchRequestErr[K ~string | ~uint64, V any](reqs []*autoBatchRequest[K, V]) error {
	for _, req := range reqs {
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

func (db *DB[K, V]) autoBatchOpName(reqs []*autoBatchRequest[K, V], atomicAll bool) string {
	if len(reqs) == 0 {
		return "batch"
	}
	if !atomicAll {
		if len(reqs) == 1 && db.autoBatcher.maxOps <= 1 {
			return singleAutoBatchOpName(reqs[0])
		}
		return "batch"
	}
	if len(reqs) == 1 {
		return singleAutoBatchOpName(reqs[0])
	}
	return batchAutoBatchOpName(reqs[0])
}

func (db *DB[K, V]) runAutoBatchAtomic(batch []*autoBatchRequest[K, V]) {
	for _, req := range batch {
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

func (db *DB[K, V]) executeAutoBatchAttempt(active []*autoBatchRequest[K, V], atomicAll bool) (*autoBatchRequest[K, V], bool, error) {
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

	att := db.autoBatcher.acquireAttemptState(db, bucket, len(active))
	defer db.autoBatcher.releaseAttemptState(att)
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

func (db *DB[K, V]) failAutoBatch(batch []*autoBatchRequest[K, V], err error) {
	assignAutoBatchRequestErr(batch, err)
	db.finishAutoBatch(batch)
}

func (db *DB[K, V]) finishAutoBatch(batch []*autoBatchRequest[K, V]) {
	resolveAutoBatchRequestErrs(batch)
	releaseAutoBatchRequestsState(batch)
	for _, req := range batch {
		req.done <- req.err
	}
}
