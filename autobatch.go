package rbi

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
	"go.etcd.io/bbolt"
)

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

var (
	errAutoBatchEmptyPayload = errors.New("empty msgpack payload")
	errAutoBatchUnknownOp    = errors.New("unknown auto-batch op")
	errAutoBatchCloneNil     = errors.New("clone returned nil")
)

type autoBatchPrepareErr uint8

const (
	autoBatchPrepareErrDecode autoBatchPrepareErr = iota + 1
	autoBatchPrepareErrDecodePreparedValue
	autoBatchPrepareErrDecodeExistingValue
	autoBatchPrepareErrRedecodeEmpty
	autoBatchPrepareErrRedecodeValue
	autoBatchPrepareErrApplyPatch
	autoBatchPrepareErrEncode
)

func formatAutoBatchPrepareErr(kind autoBatchPrepareErr, err error) error {
	switch kind {
	case autoBatchPrepareErrDecode:
		return fmt.Errorf("decode: %w", err)
	case autoBatchPrepareErrDecodePreparedValue:
		return fmt.Errorf("decode prepared value: %w", err)
	case autoBatchPrepareErrDecodeExistingValue:
		return fmt.Errorf("failed to decode existing value: %w", err)
	case autoBatchPrepareErrRedecodeEmpty:
		return errors.New("failed to re-decode value for patching: source payload is empty")
	case autoBatchPrepareErrRedecodeValue:
		return fmt.Errorf("failed to re-decode value for patching: %w", err)
	case autoBatchPrepareErrApplyPatch:
		return fmt.Errorf("failed to apply patch: %w", err)
	case autoBatchPrepareErrEncode:
		return fmt.Errorf("encode: %w", err)
	default:
		return err
	}
}

type (
	autoBatchBeforeProcessHook func(keycodec.DataKey, unsafe.Pointer) error
	autoBatchBeforeStoreHook   func(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error
	autoBatchBeforeCommitHook  func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error
	autoBatchCloneFunc         func(keycodec.DataKey, unsafe.Pointer) (unsafe.Pointer, error)
)

type autoBatchRecordOps struct {
	encode        func(unsafe.Pointer, *bytes.Buffer) error
	decode        func([]byte) (unsafe.Pointer, error)
	release       func(unsafe.Pointer)
	validateIndex func(unsafe.Pointer) error
}

type autoBatchRuntime struct {
	bolt               *bbolt.DB
	bucket             []byte
	bucketFillPercent  float64
	strKey             bool
	strMap             *strmap.Mapper
	engine             *queryEngine
	schema             *schema.Runtime
	testHookAccessor   func() *testHooks
	broken             *atomic.Bool
	logger             *log.Logger
	mu                 *sync.RWMutex
	ops                autoBatchRecordOps
	closed             *atomic.Bool
	rejectEmptyPayload bool
}

type autoBatchRequest struct {
	op autoBatchOp
	id keycodec.DataKey

	setValue    unsafe.Pointer
	setBaseline unsafe.Pointer
	setPayload  *bytes.Buffer

	patch []schema.PatchItem

	patchIgnoreUnknown bool

	beforeProcess []autoBatchBeforeProcessHook
	beforeStore   []autoBatchBeforeStoreHook
	beforeCommit  []autoBatchBeforeCommitHook
	cloneValue    autoBatchCloneFunc
	policy        autoBatchReqPolicy

	replacedBy *autoBatchRequest

	err  error
	done chan error
}

type autoBatchJob struct {
	reqs       []*autoBatchRequest
	isolated   bool
	done       chan error
	enqueuedAt int64
}

type autoBatchPrepared struct {
	req *autoBatchRequest

	key     []byte
	payload []byte
	idx     uint64
	idxNew  bool
	oldVal  unsafe.Pointer
	newVal  unsafe.Pointer
}

type autoBatchState struct {
	key    []byte
	keyBuf [8]byte

	idx      uint64
	idxKnown bool
	idxNew   bool

	value           unsafe.Pointer
	ownedPayload    *bytes.Buffer
	borrowedPayload []byte
}

type autoBatchAttemptCore struct {
	bucket       *bbolt.Bucket
	statsEnabled bool

	preparedSnapshots []snapshot.BatchEntry
	acceptedSnapshots []snapshot.BatchEntry
	ownedPayloads     []*bytes.Buffer

	uniqueIdxs    []uint64
	uniqueOldVals []unsafe.Pointer
	uniqueNewVals []unsafe.Pointer
}

type autoBatchAttemptState struct {
	autoBatchAttemptCore

	prepared []autoBatchPrepared
	accepted []autoBatchPrepared

	stateByUintID      map[uint64]int
	stateByUintIDCap   int
	stateByStringID    map[string]int
	stateByStringIDCap int
	states             []autoBatchState
}

func (core *autoBatchAttemptCore) prepare(bucket *bbolt.Bucket, statsEnabled bool, capHint int, withSnapshots bool) {
	core.bucket = bucket
	core.statsEnabled = statsEnabled
	if cap(core.ownedPayloads) < capHint {
		core.ownedPayloads = slices.Grow(core.ownedPayloads, capHint)
	}
	if cap(core.uniqueIdxs) < capHint {
		core.uniqueIdxs = slices.Grow(core.uniqueIdxs, capHint)
	}
	if cap(core.uniqueOldVals) < capHint {
		core.uniqueOldVals = slices.Grow(core.uniqueOldVals, capHint)
	}
	if cap(core.uniqueNewVals) < capHint {
		core.uniqueNewVals = slices.Grow(core.uniqueNewVals, capHint)
	}
	if withSnapshots {
		if cap(core.preparedSnapshots) < capHint {
			core.preparedSnapshots = slices.Grow(core.preparedSnapshots, capHint)
		}
		if cap(core.acceptedSnapshots) < capHint {
			core.acceptedSnapshots = slices.Grow(core.acceptedSnapshots, capHint)
		}
	}
}

func (core *autoBatchAttemptCore) cleanup() {
	for _, buf := range core.ownedPayloads {
		encodePool.Put(buf)
	}
	clear(core.ownedPayloads)
	core.ownedPayloads = core.ownedPayloads[:0]

	clear(core.preparedSnapshots)
	core.preparedSnapshots = core.preparedSnapshots[:0]

	clear(core.acceptedSnapshots)
	core.acceptedSnapshots = core.acceptedSnapshots[:0]

	core.uniqueIdxs = core.uniqueIdxs[:0]

	clear(core.uniqueOldVals)
	core.uniqueOldVals = core.uniqueOldVals[:0]

	clear(core.uniqueNewVals)
	core.uniqueNewVals = core.uniqueNewVals[:0]

	core.bucket = nil
	core.statsEnabled = false
}

func (req *autoBatchRequest) payloadBytes() []byte {
	if req.setPayload == nil {
		return nil
	}
	return req.setPayload.Bytes()
}

func (req *autoBatchRequest) hasPolicy(policy autoBatchReqPolicy) bool {
	return req.policy&policy != 0
}

func (req *autoBatchRequest) canCoalesceSetDelete() bool {
	return req.replacedBy == nil && req.hasPolicy(autoBatchReqSetDeleteCoalescible)
}

func (req *autoBatchRequest) canShareRepeatedID() bool {
	return req.replacedBy == nil && req.hasPolicy(autoBatchReqRepeatIDSafeShared)
}

func (st *autoBatchState) payloadBytes() []byte {
	if st.ownedPayload != nil {
		return st.ownedPayload.Bytes()
	}
	return st.borrowedPayload
}

func (st *autoBatchState) setOwnedPayload(buf *bytes.Buffer) {
	st.ownedPayload = buf
	st.borrowedPayload = nil
}

func (st *autoBatchState) setBorrowedPayload(payload []byte) {
	st.ownedPayload = nil
	st.borrowedPayload = payload
}

func (st *autoBatchState) clearPayload() {
	st.ownedPayload = nil
	st.borrowedPayload = nil
}

func (st *autoBatchState) setKey(rt *autoBatchRuntime, key keycodec.DataKey) {
	st.key = key.Bytes(rt.strKey, &st.keyBuf)
}

func (rt *autoBatchRuntime) unavailableErr() error {
	if rt.closed.Load() {
		return ErrClosed
	}
	if rt.broken.Load() {
		return ErrBroken
	}
	return nil
}

func (rt *autoBatchRuntime) idxFromKeyWithCreated(key keycodec.DataKey) (uint64, bool) {
	if rt.strKey {
		return rt.strMap.Create(key.String())
	}
	return key.Uint(), false
}

func (rt *autoBatchRuntime) rollbackCreatedStrIdx(key keycodec.DataKey, idx uint64) {
	if !rt.strKey {
		return
	}
	rt.strMap.RollbackCreated(key.String(), idx)
}

func (rt *autoBatchRuntime) rollbackCreated(ops []autoBatchPrepared) {
	if !rt.strKey {
		return
	}
	for i := range ops {
		op := ops[i]
		if !op.idxNew || op.oldVal != nil || op.newVal == nil {
			continue
		}
		rt.rollbackCreatedStrIdx(op.req.id, op.idx)
	}
}

func (rt *autoBatchRuntime) patchTouchesUnique(patch []schema.PatchItem) bool {
	if rt.schema == nil {
		return false
	}
	for _, p := range patch {
		if rt.schema.PatchNameTouchesUnique(p.Name) {
			return true
		}
	}
	return false
}

func (rt *autoBatchRuntime) prepareAttemptStateMap(st *autoBatchAttemptState, capHint int) {
	if rt.strKey {
		switch {
		case st.stateByStringID == nil:
			st.stateByStringID = make(map[string]int, capHint)
			st.stateByStringIDCap = capHint
		case capHint > st.stateByStringIDCap:
			st.stateByStringID = make(map[string]int, capHint)
			st.stateByStringIDCap = capHint
		}
		return
	}

	switch {
	case st.stateByUintID == nil:
		st.stateByUintID = make(map[uint64]int, capHint)
		st.stateByUintIDCap = capHint
	case capHint > st.stateByUintIDCap:
		st.stateByUintID = make(map[uint64]int, capHint)
		st.stateByUintIDCap = capHint
	}
}

func (rt *autoBatchRuntime) attemptStatePos(st *autoBatchAttemptState, key keycodec.DataKey) (int, bool) {
	if rt.strKey {
		pos, ok := st.stateByStringID[key.String()]
		return pos, ok
	}
	pos, ok := st.stateByUintID[key.Uint()]
	return pos, ok
}

func (rt *autoBatchRuntime) setAttemptStatePos(st *autoBatchAttemptState, key keycodec.DataKey, pos int) {
	if rt.strKey {
		st.stateByStringID[key.String()] = pos
		return
	}
	st.stateByUintID[key.Uint()] = pos
}

func (st *autoBatchAttemptState) loadState(rt *autoBatchRuntime, req *autoBatchRequest) (*autoBatchState, error) {
	if pos, ok := rt.attemptStatePos(st, req.id); ok {
		return &st.states[pos], nil
	}

	pos := len(st.states)
	st.states = append(st.states, autoBatchState{})
	state := &st.states[pos]
	state.setKey(rt, req.id)
	if prev := st.bucket.Get(state.key); prev != nil {
		oldVal, err := rt.ops.decode(prev)
		if err != nil {
			st.states = st.states[:pos]
			return nil, err
		}
		state.value = oldVal
		state.setBorrowedPayload(prev)
	}

	rt.setAttemptStatePos(st, req.id, pos)
	return state, nil
}

func (st *autoBatchAttemptState) ensureIdx(rt *autoBatchRuntime, id keycodec.DataKey, state *autoBatchState, create bool) {
	if state.idxKnown {
		return
	}
	if create {
		state.idx, state.idxNew = rt.idxFromKeyWithCreated(id)
	} else {
		state.idx, _ = rt.idxFromKeyWithCreated(id)
	}
	state.idxKnown = true
}

func (ab *autoBatchScheduler) queueLen() int {
	return ab.queueSize
}

func (ab *autoBatchScheduler) queueIndex(i int) int {
	idx := ab.queueHead + i
	if idx >= len(ab.queue) {
		idx -= len(ab.queue)
	}
	return idx
}

func (ab *autoBatchScheduler) queueAt(i int) *autoBatchJob {
	return ab.queue[ab.queueIndex(i)]
}

func (ab *autoBatchScheduler) growQueue() {
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
	grown := make([]*autoBatchJob, newCap)
	for i := 0; i < ab.queueSize; i++ {
		grown[i] = ab.queueAt(i)
	}
	ab.queue = grown
	ab.queueHead = 0
}

func (ab *autoBatchScheduler) enqueue(job *autoBatchJob) {
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

func (ab *autoBatchScheduler) dequeueFrontScratch(n int) []*autoBatchJob {
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

func runBeforeStoreHooks(id keycodec.DataKey, oldVal, newVal unsafe.Pointer, hooks []autoBatchBeforeStoreHook) error {
	for _, fn := range hooks {
		if err := fn(id, oldVal, newVal); err != nil {
			return err
		}
	}
	return nil
}

func runBeforeCommitHooks(tx *bbolt.Tx, id keycodec.DataKey, oldVal, newVal unsafe.Pointer, hooks []autoBatchBeforeCommitHook) error {
	for _, fn := range hooks {
		if err := fn(tx, id, oldVal, newVal); err != nil {
			return err
		}
	}
	return nil
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

func assignAutoBatchPreparedErr(ops []autoBatchPrepared, err error) {
	for _, op := range ops {
		if op.req.err == nil {
			op.req.err = err
		}
	}
}

func assignAutoBatchRequestErr(reqs []*autoBatchRequest, err error) {
	for i := 0; i < len(reqs); i++ {
		req := reqs[i]
		if req.err == nil {
			req.err = err
		}
	}
}

func (db *DB[K, V]) buildSetAutoBatchRequest(key keycodec.DataKey, newVal *V, beforeStore []autoBatchBeforeStoreHook, beforeCommit []autoBatchBeforeCommitHook, cloneValue autoBatchCloneFunc) (*autoBatchRequest, error) {
	if newVal == nil {
		return nil, ErrNilValue
	}

	req := db.autoBatcher.requestPool.Get()
	req.op = autoBatchSet
	req.id = key
	req.beforeStore = beforeStore
	req.beforeCommit = beforeCommit
	req.cloneValue = cloneValue
	if len(beforeStore) > 0 && cloneValue != nil {
		var err error
		if req.setBaseline, err = cloneValue(req.id, unsafe.Pointer(newVal)); err != nil {
			db.autoBatcher.requestPool.Put(req)
			return nil, err
		}
	} else {
		b := encodePool.Get()
		if err := db.autoBatcher.runtime.ops.encode(unsafe.Pointer(newVal), b); err != nil {
			encodePool.Put(b)
			db.autoBatcher.requestPool.Put(req)
			return nil, formatAutoBatchPrepareErr(autoBatchPrepareErrEncode, err)
		}
		req.setValue = unsafe.Pointer(newVal)
		req.setPayload = b
	}
	if len(beforeStore) == 0 && len(beforeCommit) == 0 {
		req.policy = autoBatchReqSetDeleteCoalescible
	}
	return req, nil
}

func (db *DB[K, V]) buildPatchAutoBatchRequest(id K, fields []Field, ignoreUnknown bool, beforeProcess []autoBatchBeforeProcessHook, beforeStore []autoBatchBeforeStoreHook, beforeCommit []autoBatchBeforeCommitHook) *autoBatchRequest {
	req := db.autoBatcher.requestPool.Get()
	req.op = autoBatchPatch
	req.id = keycodec.DataKeyFromUserKey(id, db.strKey)
	if cap(req.patch) < len(fields) {
		req.patch = slices.Grow(req.patch, len(fields))
	}
	req.patch = req.patch[:len(fields)]
	for i := range fields {
		req.patch[i] = schema.PatchItem(fields[i])
	}
	req.patchIgnoreUnknown = ignoreUnknown
	req.beforeProcess = beforeProcess
	req.beforeStore = beforeStore
	req.beforeCommit = beforeCommit
	rt := db.autoBatcher.runtime
	if rt.engine == nil || !rt.engine.schema.HasUnique {
		req.policy = autoBatchReqRepeatIDSafeShared
	} else if len(beforeProcess) == 0 && len(beforeStore) == 0 && !rt.patchTouchesUnique(req.patch) {
		req.policy = autoBatchReqRepeatIDSafeShared
	}
	return req
}

func (db *DB[K, V]) buildDeleteAutoBatchRequest(id K, beforeCommit []autoBatchBeforeCommitHook) *autoBatchRequest {
	req := db.autoBatcher.requestPool.Get()
	req.op = autoBatchDelete
	req.id = keycodec.DataKeyFromUserKey(id, db.strKey)
	req.beforeCommit = beforeCommit
	req.policy = autoBatchReqRepeatIDSafeShared
	if len(beforeCommit) == 0 {
		req.policy |= autoBatchReqSetDeleteCoalescible
	}
	return req
}

func (ab *autoBatcher) submitAutoBatchRequests(reqs []*autoBatchRequest, isolated bool) error {
	if len(reqs) == 0 {
		return nil
	}

	stats := ab.statsEnabled
	if stats {
		ab.submitted.Add(1)
	}
	if err := ab.runtime.unavailableErr(); err != nil {
		for i := 0; i < len(reqs); i++ {
			ab.requestPool.Put(reqs[i])
		}
		if stats {
			ab.fallbackClosed.Add(1)
		}
		return err
	}

	job := ab.jobPool.Get()
	job.reqs = reqs
	job.isolated = isolated || len(reqs) != 1

	ab.mu.Lock()
	for {
		if err := ab.runtime.unavailableErr(); err != nil {
			ab.mu.Unlock()
			for i := 0; i < len(reqs); i++ {
				ab.requestPool.Put(reqs[i])
			}
			ab.jobPool.Put(job)
			if stats {
				ab.fallbackClosed.Add(1)
			}
			return err
		}
		if !ab.running {
			ab.running = true
			go ab.runAutoBatcherLoop()
		}
		if ab.maxQ <= 0 || ab.queueSize < ab.maxQ {
			break
		}
		ab.cond.Wait()
	}

	ab.enqueue(job)
	select {
	case ab.waitNotify <- struct{}{}:
	default:
	}
	if ab.statsEnabled {
		job.enqueuedAt = time.Now().UnixNano()
		ab.enqueued.Add(1)
		atomicSetMax(&ab.queueHighWater, uint64(ab.queueSize))
	}
	ab.mu.Unlock()

	err := <-job.done
	for i := 0; i < len(reqs); i++ {
		ab.requestPool.Put(reqs[i])
	}
	ab.jobPool.Put(job)
	return err
}

func (ab *autoBatcher) runAutoBatcherLoop() {
	for {
		batch := ab.popAutoBatch()
		if len(batch) == 0 {
			return
		}
		if err := ab.runtime.unavailableErr(); err != nil {
			ab.failAutoBatchJobs(batch, err)
			clear(batch)
			ab.mu.Lock()
			ab.recycleBatchScratchLocked(batch)
			ab.mu.Unlock()
			continue
		}
		ab.executeAutoBatchJobs(batch)
		clear(batch)
		ab.mu.Lock()
		ab.recycleBatchScratchLocked(batch)
		ab.mu.Unlock()
	}
}

func (ab *autoBatchScheduler) frontLimit() int {
	limit := min(ab.maxOps, ab.queueSize)
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

func autoBatchRepeatedIDCompatible(prev, req *autoBatchRequest) bool {
	return (req.canCoalesceSetDelete() && prev.canCoalesceSetDelete()) ||
		(req.canShareRepeatedID() && prev.canShareRepeatedID())
}

func (ab *autoBatchScheduler) waitDurationLocked(frontLimit int) time.Duration {
	if ab.window <= 0 || ab.queueLen() >= ab.maxOps {
		return 0
	}

	now := time.Now()
	switch {

	case frontLimit > 1:
		return ab.window

	case ab.queueLen() == 1 &&
		(ab.hotBatchSize == 0 || ab.hotBatchSize >= 3) &&
		now.Before(ab.hotUntil):
		waitDur := ab.window
		if waitDur > 50*time.Microsecond {
			waitDur /= 2
		}
		return waitDur

	default:
		return 0
	}
}

func (ab *autoBatchScheduler) waitAutoBatchWindowLocked(frontLimit int) bool {
	waitDur := ab.waitDurationLocked(frontLimit)
	if waitDur <= 0 {
		return true
	}

	start := time.Now()
	ab.mu.Unlock()

	if frontLimit > 1 {
		time.Sleep(waitDur)
	} else {
		deadline := start.Add(waitDur)
		timer := ab.waitTimer

		for {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			if timer == nil {
				timer = time.NewTimer(remaining)
				ab.waitTimer = timer
			} else {
				timer.Reset(remaining)
			}

			select {
			case <-timer.C:
			case <-ab.waitNotify:
			}
			timer.Stop()

			ab.mu.Lock()
			if ab.queueSize == 0 {
				if ab.statsEnabled {
					ab.coalesceWaits.Add(1)
					ab.coalesceWaitNanos.Add(uint64(time.Since(start)))
				}
				ab.running = false
				ab.mu.Unlock()
				return false
			}
			if ab.hotBatchSize > 0 && ab.queueSize >= ab.hotBatchSize {
				ab.mu.Unlock()
				break
			}
			ab.mu.Unlock()
		}
	}

	if ab.statsEnabled {
		ab.coalesceWaits.Add(1)
		ab.coalesceWaitNanos.Add(uint64(time.Since(start)))
	}
	ab.mu.Lock()
	if ab.queueSize == 0 {
		ab.running = false
		ab.mu.Unlock()
		return false
	}
	return true
}

func (ab *autoBatchScheduler) updateHotAfterDequeueLocked(n int, now time.Time) {
	if n > 1 {
		keepHot := 4 * ab.window
		if keepHot < 500*time.Microsecond {
			keepHot = 500 * time.Microsecond
		}
		ab.hotUntil = now.Add(keepHot)
		ab.hotBatchSize = n
		return
	}
	ab.hotUntil = time.Time{}
	ab.hotBatchSize = 0
}

func (ab *autoBatchScheduler) recordDequeuedStatsLocked(batch []*autoBatchJob, now time.Time) {
	if !ab.statsEnabled {
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
	ab.dequeued.Add(uint64(len(batch)))
	ab.queueWaitNanos.Add(queueWait)
}

func (ab *autoBatchScheduler) recycleBatchScratchLocked(batch []*autoBatchJob) {
	if cap(batch) > cap(ab.batchScratch) {
		ab.batchScratch = batch[:0]
	} else if ab.batchScratch == nil {
		ab.batchScratch = batch[:0]
	}
}

func (ab *autoBatcher) autoBatchRepeatedIDLimitLocked(limit int) int {
	if limit <= 1 {
		return limit
	}

	if ab.runtime.strKey {
		lastByID := ab.repeatStringIDPool.Get(limit)
		for i := 0; i < limit; i++ {
			req := ab.queueAt(i).reqs[0]
			id := req.id.String()

			prevIdx, seen := lastByID[id]
			if seen {
				prev := ab.queueAt(prevIdx).reqs[0]
				if !autoBatchRepeatedIDCompatible(prev, req) {
					limit = i
					break
				}
			}
			lastByID[id] = i
		}
		ab.repeatStringIDPool.Put(lastByID)
		return limit
	}

	lastByID := ab.repeatUintIDPool.Get(limit)
	for i := 0; i < limit; i++ {
		req := ab.queueAt(i).reqs[0]
		id := req.id.Uint()

		prevIdx, seen := lastByID[id]
		if seen {
			prev := ab.queueAt(prevIdx).reqs[0]
			if !autoBatchRepeatedIDCompatible(prev, req) {
				limit = i
				break
			}
		}
		lastByID[id] = i
	}
	ab.repeatUintIDPool.Put(lastByID)
	return limit
}

func (ab *autoBatcher) popAutoBatch() []*autoBatchJob {
	ab.mu.Lock()
	if ab.queueSize == 0 {
		ab.running = false
		ab.mu.Unlock()
		return nil
	}

	frontLimit := ab.frontLimit()
	if !ab.waitAutoBatchWindowLocked(frontLimit) {
		return nil
	}

	n := ab.frontLimit()
	n = ab.autoBatchRepeatedIDLimitLocked(n)

	batch := ab.dequeueFrontScratch(n)
	now := time.Now()
	ab.updateHotAfterDequeueLocked(n, now)
	if len(batch) > 1 {
		ab.markSupersededSetDeleteJobs(batch)
	}
	ab.recordDequeuedStatsLocked(batch, now)
	if ab.cond != nil {
		ab.cond.Broadcast()
	}
	ab.mu.Unlock()
	return batch
}

func (ab *autoBatchScheduler) recordExecutedStats(size int) {
	if !ab.statsEnabled {
		return
	}
	ab.executedBatches.Add(1)
	if size > 1 {
		ab.multiReqBatches.Add(1)
		ab.multiReqOps.Add(uint64(size))
	}
	switch {
	case size <= 1:
		ab.batchSize1.Add(1)
	case size <= 4:
		ab.batchSize2To4.Add(1)
	case size <= 8:
		ab.batchSize5To8.Add(1)
	default:
		ab.batchSize9Plus.Add(1)
	}
	atomicSetMax(&ab.maxBatchSeen, uint64(size))
}

func (ab *autoBatcher) markSupersededSetDeleteJobs(batch []*autoBatchJob) {
	if len(batch) < 2 {
		return
	}
	stats := ab.statsEnabled

	if ab.runtime.strKey {
		lastByID := ab.repeatStringIDPool.Get(len(batch))
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
						ab.coalescedSetDelete.Add(1)
					}
				}
			}
			lastByID[id] = i
		}
		ab.repeatStringIDPool.Put(lastByID)
		return
	}

	lastByID := ab.repeatUintIDPool.Get(len(batch))
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
					ab.coalescedSetDelete.Add(1)
				}
			}
		}
		lastByID[id] = i
	}
	ab.repeatUintIDPool.Put(lastByID)
}

func (ab *autoBatcher) prepareAutoBatchActive(att *autoBatchAttemptState, active []*autoBatchRequest) {
	for i := 0; i < len(active); i++ {
		req := active[i]
		if req.err != nil {
			continue
		}

		switch req.op {
		case autoBatchSet:
			ab.prepareAutoBatchSet(att, req)
		case autoBatchPatch:
			ab.prepareAutoBatchPatch(att, req)
		case autoBatchDelete:
			ab.prepareAutoBatchDelete(att, req)
		}
	}
}

func (ab *autoBatcher) prepareAutoBatchSet(att *autoBatchAttemptState, req *autoBatchRequest) {
	rt := ab.runtime
	state, err := att.loadState(rt, req)
	if err != nil {
		req.err = formatAutoBatchPrepareErr(autoBatchPrepareErrDecode, err)
		return
	}

	oldVal := state.value
	newVal := req.setValue
	payload := req.payloadBytes()
	var ownedPayload *bytes.Buffer
	releaseDecodedNewVal := false
	defer func() {
		if releaseDecodedNewVal {
			rt.ops.release(newVal)
		}
	}()

	if len(req.beforeStore) > 0 {
		if att.statsEnabled {
			ab.callbackOps.Add(1)
		}
		if req.cloneValue != nil {
			newVal, err = req.cloneValue(req.id, req.setBaseline)
			if err != nil {
				req.err = err
				return
			}
		} else {
			newVal, err = rt.ops.decode(req.payloadBytes())
			if err != nil {
				req.err = formatAutoBatchPrepareErr(autoBatchPrepareErrDecodePreparedValue, err)
				return
			}
			releaseDecodedNewVal = true
		}
		if err = runBeforeStoreHooks(req.id, oldVal, newVal, req.beforeStore); err != nil {
			req.err = err
			if att.statsEnabled {
				ab.callbackErrors.Add(1)
			}
			return
		}

		if err = rt.ops.validateIndex(newVal); err != nil {
			req.err = err
			return
		}

		buf := encodePool.Get()
		if err = rt.ops.encode(newVal, buf); err != nil {
			encodePool.Put(buf)
			req.err = formatAutoBatchPrepareErr(autoBatchPrepareErrEncode, err)
			return
		}
		ownedPayload = buf
		att.ownedPayloads = append(att.ownedPayloads, buf)
		payload = buf.Bytes()
	}

	if len(req.beforeStore) == 0 {
		if err = rt.ops.validateIndex(newVal); err != nil {
			req.err = err
			return
		}
	}

	if rt.engine != nil {
		att.ensureIdx(rt, req.id, state, true)
	}

	att.prepared = append(att.prepared, autoBatchPrepared{
		req:     req,
		key:     state.key,
		payload: payload,
		idx:     state.idx,
		idxNew:  state.idxNew,
		oldVal:  oldVal,
		newVal:  newVal,
	})
	if rt.engine != nil {
		att.preparedSnapshots = append(att.preparedSnapshots, snapshot.BatchEntry{
			ID:  state.idx,
			Old: oldVal,
			New: newVal,
		})
	}
	state.value = newVal
	releaseDecodedNewVal = false
	if ownedPayload != nil {
		state.setOwnedPayload(ownedPayload)
	} else {
		state.setBorrowedPayload(payload)
	}
}

func (ab *autoBatcher) prepareAutoBatchPatch(att *autoBatchAttemptState, req *autoBatchRequest) {
	rt := ab.runtime
	state, err := att.loadState(rt, req)
	if err != nil {
		req.err = formatAutoBatchPrepareErr(autoBatchPrepareErrDecodeExistingValue, err)
		return
	}
	if state.value == nil {
		return
	}
	if state.ownedPayload == nil && state.borrowedPayload == nil {
		req.err = formatAutoBatchPrepareErr(autoBatchPrepareErrRedecodeEmpty, nil)
		return
	}

	oldVal := state.value
	newVal, err := rt.ops.decode(state.payloadBytes())
	if err != nil {
		req.err = formatAutoBatchPrepareErr(autoBatchPrepareErrRedecodeValue, err)
		return
	}
	releaseDecodedNewVal := true
	defer func() {
		if releaseDecodedNewVal {
			rt.ops.release(newVal)
		}
	}()
	if err = rt.schema.Patch.Apply(newVal, req.patch, req.patchIgnoreUnknown); err != nil {
		req.err = formatAutoBatchPrepareErr(autoBatchPrepareErrApplyPatch, err)
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
			ab.callbackOps.Add(1)
		}
		if err = runBeforeStoreHooks(req.id, oldVal, newVal, req.beforeStore); err != nil {
			req.err = err
			if att.statsEnabled {
				ab.callbackErrors.Add(1)
			}
			return
		}
	}

	if err = rt.ops.validateIndex(newVal); err != nil {
		req.err = err
		return
	}

	buf := encodePool.Get()
	if err = rt.ops.encode(newVal, buf); err != nil {
		encodePool.Put(buf)
		req.err = formatAutoBatchPrepareErr(autoBatchPrepareErrEncode, err)
		return
	}
	att.ownedPayloads = append(att.ownedPayloads, buf)

	if rt.engine != nil {
		att.ensureIdx(rt, req.id, state, false)
	}

	att.prepared = append(att.prepared, autoBatchPrepared{
		req:     req,
		key:     state.key,
		payload: buf.Bytes(),
		idx:     state.idx,
		idxNew:  state.idxNew,
		oldVal:  oldVal,
		newVal:  newVal,
	})
	if rt.engine != nil {
		att.preparedSnapshots = append(att.preparedSnapshots, snapshot.BatchEntry{
			ID:        state.idx,
			Old:       oldVal,
			New:       newVal,
			Patch:     req.patch,
			PatchOnly: len(req.beforeProcess) == 0 && len(req.beforeStore) == 0,
		})
	}
	state.value = newVal
	releaseDecodedNewVal = false
	state.setOwnedPayload(buf)
}

func (ab *autoBatcher) prepareAutoBatchDelete(att *autoBatchAttemptState, req *autoBatchRequest) {
	rt := ab.runtime
	state, err := att.loadState(rt, req)
	if err != nil {
		req.err = formatAutoBatchPrepareErr(autoBatchPrepareErrDecode, err)
		return
	}
	if state.value == nil {
		return
	}

	oldVal := state.value
	if rt.engine != nil {
		att.ensureIdx(rt, req.id, state, false)
	}

	att.prepared = append(att.prepared, autoBatchPrepared{
		req:    req,
		key:    state.key,
		idx:    state.idx,
		idxNew: state.idxNew,
		oldVal: oldVal,
		newVal: nil,
	})
	if rt.engine != nil {
		att.preparedSnapshots = append(att.preparedSnapshots, snapshot.BatchEntry{
			ID:  state.idx,
			Old: oldVal,
		})
	}
	state.value = nil
	state.clearPayload()
}

func (ab *autoBatcher) filterAutoBatchAcceptedLocked(att *autoBatchAttemptState, atomicAll bool) error {
	rt := ab.runtime
	att.accepted = att.prepared
	att.acceptedSnapshots = att.preparedSnapshots
	if rt.engine == nil || !rt.engine.schema.HasUnique {
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
		if err := rt.engine.checkUniqueOnWriteMulti(att.uniqueIdxs, att.uniqueOldVals, att.uniqueNewVals); err != nil {
			if ab.statsEnabled {
				ab.uniqueRejected.Add(1)
			}
			for _, op := range att.prepared {
				op.req.err = err
			}
			return err
		}
		return nil
	}

	att.accepted = slices.Grow(att.accepted[:0], len(att.prepared))[:0]
	att.acceptedSnapshots = slices.Grow(att.acceptedSnapshots[:0], len(att.prepared))[:0]
	uniqueState := uniqueBatchCheckState{
		leaving: uniqueLeavingOuterPool.Get(),
		seen:    uniqueSeenOuterPool.Get(),
	}
	defer uniqueLeavingOuterPool.Put(uniqueState.leaving)
	defer uniqueSeenOuterPool.Put(uniqueState.seen)

	for i := range att.prepared {
		op := att.prepared[i]
		if err := rt.engine.checkUniqueBatchAppend(uniqueState, op.idx, op.oldVal, op.newVal); err != nil {
			if ab.statsEnabled {
				ab.uniqueRejected.Add(1)
			}
			op.req.err = err
			if rt.strKey && op.idxNew && op.oldVal == nil && op.newVal != nil {
				rt.rollbackCreatedStrIdx(op.req.id, op.idx)
			}
			continue
		}
		att.accepted = append(att.accepted, op)
		att.acceptedSnapshots = append(att.acceptedSnapshots, att.preparedSnapshots[i])
	}
	return nil
}

func formatAutoBatchBoltWriteErr(err error, op autoBatchOp, id string, idx uint64, key, payload []byte) error {
	switch {
	case errors.Is(err, errAutoBatchEmptyPayload):
		return fmt.Errorf(
			"invalid write payload op=%s id=%s idx=%d key_len=%d payload_len=0: empty msgpack payload",
			autoBatchOpString(op),
			id,
			idx,
			len(key),
		)
	case errors.Is(err, errAutoBatchUnknownOp):
		return fmt.Errorf("unknown auto-batch op: %v", op)
	case op == autoBatchSet || op == autoBatchPatch:
		return fmt.Errorf(
			"put op=%s id=%s idx=%d key_len=%d payload_len=%d payload_prefix_hex=%s: %w",
			autoBatchOpString(op),
			id,
			idx,
			len(key),
			len(payload),
			formatDiagnosticBytesPrefix(payload, 32),
			err,
		)
	case op == autoBatchDelete:
		return fmt.Errorf(
			"delete op=%s id=%s idx=%d key_len=%d: %w",
			autoBatchOpString(op),
			id,
			idx,
			len(key),
			err,
		)
	default:
		return fmt.Errorf("unknown auto-batch op: %v", op)
	}
}

func (ab *autoBatcher) applyAutoBatchAcceptedLocked(
	tx *bbolt.Tx,
	bucket *bbolt.Bucket,
	accepted []autoBatchPrepared,
	atomicAll bool,
) (*autoBatchRequest, error) {
	stats := ab.statsEnabled
	rejectEmptyPayload := ab.runtime.rejectEmptyPayload
	var callbackFailedReq *autoBatchRequest

acceptedLoop:
	for _, op := range accepted {
		var err error
		switch op.req.op {
		case autoBatchSet, autoBatchPatch:
			if rejectEmptyPayload && len(op.payload) == 0 {
				err = errAutoBatchEmptyPayload
			} else {
				err = bucket.Put(op.key, op.payload)
			}
		case autoBatchDelete:
			err = bucket.Delete(op.key)
		default:
			err = errAutoBatchUnknownOp
		}
		if err != nil {
			rawErr := err
			err = formatAutoBatchBoltWriteErr(err, op.req.op, op.req.id.Format(ab.runtime.strKey), op.idx, op.key, op.payload)
			if errors.Is(rawErr, errAutoBatchEmptyPayload) {
				op.req.err = err
			}
			if stats {
				ab.txOpErrors.Add(1)
			}
			if errors.Is(rawErr, errAutoBatchEmptyPayload) && !atomicAll {
				callbackFailedReq = op.req
				break acceptedLoop
			}
			return nil, err
		}

		if len(op.req.beforeCommit) > 0 {
			if stats && len(op.req.beforeStore) == 0 {
				ab.callbackOps.Add(1)
			}
			if err = runBeforeCommitHooks(tx, op.req.id, op.oldVal, op.newVal, op.req.beforeCommit); err != nil {
				err = fmt.Errorf(
					"before_commit op=%s id=%s idx=%d key_len=%d payload_len=%d: %w",
					autoBatchOpString(op.req.op),
					op.req.id.Format(ab.runtime.strKey),
					op.idx,
					len(op.key),
					len(op.payload),
					err,
				)
				op.req.err = err
				if stats {
					ab.callbackErrors.Add(1)
					ab.txOpErrors.Add(1)
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

func (ab *autoBatcher) executeAutoBatchJobs(batch []*autoBatchJob) {
	ab.autoBatchScheduler.recordExecutedStats(len(batch))
	stats := ab.statsEnabled
	var started time.Time
	if stats {
		started = time.Now()
	}

	if len(batch) == 1 && batch[0].isolated {
		ab.runAutoBatchAtomic(batch[0].reqs)
		ab.finishAutoBatchJobs(batch)
		if stats {
			ab.executeNanos.Add(uint64(time.Since(started)))
		}
		return
	}

	reqScratch := ab.requestScratchPool.Get(len(batch))

	for _, job := range batch {
		reqs := job.reqs
		if len(reqs) != 1 {
			assignAutoBatchRequestErr(reqs, fmt.Errorf("internal auto-batch error: mixed grouped request in shared batch"))
			continue
		}
		reqScratch = append(reqScratch, reqs[0])
	}
	if len(reqScratch) != 0 {
		ab.runAutoBatchShared(reqScratch)
	}
	ab.finishAutoBatchJobs(batch)
	ab.requestScratchPool.Put(reqScratch)
	if stats {
		ab.executeNanos.Add(uint64(time.Since(started)))
	}
}

func resolveAutoBatchRequestErrs(batch []*autoBatchRequest) {
	for i := 0; i < len(batch); i++ {
		req := batch[i]
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

func (ab *autoBatcher) finishAutoBatchJobs(batch []*autoBatchJob) {
	for _, job := range batch {
		resolveAutoBatchRequestErrs(job.reqs)
	}
	for _, job := range batch {
		var err error
		if len(job.reqs) == 1 {
			err = job.reqs[0].err
		} else {
			err = firstAutoBatchRequestErr(job.reqs)
		}
		job.done <- err
	}
}

func (ab *autoBatcher) executeAutoBatch(batch []*autoBatchRequest) {
	ab.autoBatchScheduler.recordExecutedStats(len(batch))
	stats := ab.statsEnabled
	var started time.Time
	if stats {
		started = time.Now()
	}
	reqScratch := ab.requestScratchPool.Get(len(batch))
	reqScratch = append(reqScratch, batch...)
	ab.runAutoBatchShared(reqScratch)
	ab.requestScratchPool.Put(reqScratch)
	ab.finishAutoBatch(batch)
	if stats {
		ab.executeNanos.Add(uint64(time.Since(started)))
	}
}

func (ab *autoBatcher) runAutoBatchShared(batch []*autoBatchRequest) {
	for i := 0; i < len(batch); i++ {
		batch[i].err = nil
	}

	activeScratch := ab.requestScratchPool.Get(len(batch))
	for i := 0; i < len(batch); i++ {
		req := batch[i]
		if req.replacedBy == nil {
			activeScratch = append(activeScratch, req)
		}
	}
	defer ab.requestScratchPool.Put(activeScratch)

	for {
		retryWithoutReq, done, fatalErr := ab.executeAutoBatchAttempt(activeScratch, false)
		if fatalErr != nil {
			assignAutoBatchRequestErr(batch, fatalErr)
			return
		}
		if done {
			return
		}

		write := 0
		removed := false
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
		if !removed {
			assignAutoBatchRequestErr(batch, fmt.Errorf("internal auto-batch retry error: failed request not found"))
			return
		}
		if len(activeScratch) == 0 {
			return
		}
		for i := 0; i < len(activeScratch); i++ {
			req := activeScratch[i]
			if errors.Is(req.err, ErrUniqueViolation) {
				req.err = nil
			}
		}
	}
}

func firstAutoBatchRequestErr(reqs []*autoBatchRequest) error {
	for i := 0; i < len(reqs); i++ {
		req := reqs[i]
		if req.err != nil {
			return req.err
		}
	}
	return nil
}

func autoBatchOpName(op autoBatchOp, reqLen int, atomicAll bool, maxOps int) string {
	if reqLen == 0 {
		return "batch"
	}
	if !atomicAll {
		if reqLen != 1 || maxOps > 1 {
			return "batch"
		}
		switch op {
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
	if reqLen == 1 {
		switch op {
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
	switch op {
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

func (ab *autoBatcher) runAutoBatchAtomic(batch []*autoBatchRequest) {
	for i := 0; i < len(batch); i++ {
		req := batch[i]
		req.err = nil
		req.replacedBy = nil
	}
	_, done, fatalErr := ab.executeAutoBatchAttempt(batch, true)
	if fatalErr != nil {
		assignAutoBatchRequestErr(batch, fatalErr)
		return
	}
	if !done {
		assignAutoBatchRequestErr(batch, fmt.Errorf("internal auto-batch atomic retry error"))
	}
}

func (ab *autoBatcher) executeAutoBatchAttempt(active []*autoBatchRequest, atomicAll bool) (*autoBatchRequest, bool, error) {
	rt := ab.runtime
	var firstOp autoBatchOp
	if len(active) != 0 {
		firstOp = active[0].op
	}
	opName := autoBatchOpName(firstOp, len(active), atomicAll, ab.maxOps)

	tx, err := rt.bolt.Begin(true)
	if err != nil {
		if ab.statsEnabled {
			ab.txBeginErrors.Add(1)
		}
		return nil, true, fmt.Errorf("tx error: %w", err)
	}
	bucket := tx.Bucket(rt.bucket)
	if bucket == nil {
		if ab.statsEnabled {
			ab.txOpErrors.Add(1)
		}
		rollback(tx)
		return nil, true, fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = rt.bucketFillPercent
	defer rollback(tx)

	capHint := max(len(active), 1)
	if capHint <= 8 {
		capHint = 8
	} else {
		capHint = 1 << bits.Len(uint(capHint-1))
	}
	att := ab.attemptStatePool.Get()
	att.autoBatchAttemptCore.prepare(bucket, ab.statsEnabled, capHint, rt.engine != nil)
	if cap(att.prepared) < capHint {
		att.prepared = slices.Grow(att.prepared, capHint)
	}
	if cap(att.accepted) < capHint {
		att.accepted = slices.Grow(att.accepted, capHint)
	}
	if cap(att.states) < capHint {
		att.states = slices.Grow(att.states, capHint)
	}
	rt.prepareAttemptStateMap(att, capHint)
	defer ab.attemptStatePool.Put(att)
	ab.prepareAutoBatchActive(att, active)

	if atomicAll {
		if reqErr := firstAutoBatchRequestErr(active); reqErr != nil {
			rt.rollbackCreated(att.prepared)
			return nil, true, reqErr
		}
	}

	if len(att.prepared) == 0 {
		return nil, true, nil
	}

	rt.mu.Lock()
	if err = rt.unavailableErr(); err != nil {
		rt.rollbackCreated(att.prepared)
		rt.mu.Unlock()
		assignAutoBatchPreparedErr(att.prepared, err)
		return nil, true, nil
	}

	if err = ab.filterAutoBatchAcceptedLocked(att, atomicAll); err != nil {
		rt.rollbackCreated(att.prepared)
		rt.mu.Unlock()
		return nil, true, err
	}

	if len(att.accepted) == 0 {
		rt.mu.Unlock()
		return nil, true, nil
	}

	retryWithoutReq, err := ab.applyAutoBatchAcceptedLocked(tx, bucket, att.accepted, atomicAll)
	if err != nil {
		rt.rollbackCreated(att.accepted)
		rt.mu.Unlock()
		return nil, true, err
	}
	if retryWithoutReq != nil {
		rt.rollbackCreated(att.accepted)
		rt.mu.Unlock()
		return retryWithoutReq, false, nil
	}

	if rt.engine == nil {
		if _, err = bucket.NextSequence(); err != nil {
			if ab.statsEnabled {
				ab.txOpErrors.Add(1)
			}
			rt.rollbackCreated(att.accepted)
			rt.mu.Unlock()
			return nil, true, fmt.Errorf("advance bucket sequence: %w", err)
		}
		if err = commitTx(tx, opName, rt.testHookAccessor()); err != nil {
			if ab.statsEnabled {
				ab.txCommitErrors.Add(1)
			}
			rt.mu.Unlock()
			assignAutoBatchPreparedErr(att.accepted, err)
			return nil, true, nil
		}
		rt.mu.Unlock()
		return nil, true, nil
	}

	seq, err := bucket.NextSequence()
	if err != nil {
		if ab.statsEnabled {
			ab.txOpErrors.Add(1)
		}
		rt.rollbackCreated(att.accepted)
		rt.mu.Unlock()
		return nil, true, fmt.Errorf("advance bucket sequence: %w", err)
	}
	snap := snapshot.BuildPrepared(seq, rt.engine.snapshot.Current(), rt.schema, rt.engine.snapshotCacheConfig(), rt.strMap, rt.schema.Patch.Fields, att.acceptedSnapshots)
	rt.engine.snapshot.Stage(snap)

	if err = commitTx(tx, opName, rt.testHookAccessor()); err != nil {
		if ab.statsEnabled {
			ab.txCommitErrors.Add(1)
		}
		rt.engine.snapshot.DropStaged(seq)
		rt.rollbackCreated(att.accepted)
		rt.mu.Unlock()
		assignAutoBatchPreparedErr(att.accepted, err)
		return nil, true, nil
	}

	err = publishAfterCommitLocked(
		rt.testHookAccessor(),
		rt.broken,
		rt.logger,
		rt.engine,
		seq,
		opName,
		func() {
			rt.engine.installViewNoLock(snap)
			if rt.strMap != nil {
				rt.strMap.MarkCommittedPublished(snap.StrMap)
			}
		},
	)
	if err != nil {
		rt.mu.Unlock()
		assignAutoBatchPreparedErr(att.accepted, err)
		return nil, true, nil
	}
	rt.mu.Unlock()

	return nil, true, nil
}

func (ab *autoBatcher) failAutoBatchJobs(batch []*autoBatchJob, err error) {
	for _, job := range batch {
		assignAutoBatchRequestErr(job.reqs, err)
	}
	ab.finishAutoBatchJobs(batch)
}

func (ab *autoBatcher) finishAutoBatch(batch []*autoBatchRequest) {
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
