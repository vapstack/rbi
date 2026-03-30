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

type autoBatchOp uint8

const (
	autoBatchSet autoBatchOp = iota + 1
	autoBatchPatch
	autoBatchDelete
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

	coalescedTo *autoBatchRequest[K, V]

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

	key      []byte
	payload  []byte
	idx      uint64
	idxNew   bool
	oldVal   *V
	newVal   *V
	modified []string
}

type autoBatchState[K ~string | ~uint64, V any] struct {
	key []byte

	idx      uint64
	idxKnown bool
	idxNew   bool

	current  *V
	owned    *bytes.Buffer
	borrowed []byte
}

func (req *autoBatchRequest[K, V]) payloadBytes() []byte {
	if req.setPayload == nil {
		return nil
	}
	return req.setPayload.Bytes()
}

func (req *autoBatchRequest[K, V]) releaseResources() {
	if req.setPayload != nil {
		releaseEncodeBuf(req.setPayload)
		req.setPayload = nil
	}
	req.setValue = nil
	req.setBaseline = nil
	req.patch = nil
	req.patchIgnoreUnknown = false
	req.beforeProcess = nil
	req.beforeStore = nil
	req.beforeCommit = nil
	req.cloneValue = nil
	req.coalescedTo = nil
}

func releaseAutoBatchRequestResources[K ~string | ~uint64, V any](reqs []*autoBatchRequest[K, V]) {
	for i := range reqs {
		reqs[i].releaseResources()
	}
}

func (st *autoBatchState[K, V]) payloadBytes() []byte {
	if st.owned != nil {
		return st.owned.Bytes()
	}
	return st.borrowed
}

func (st *autoBatchState[K, V]) setOwnedPayload(buf *bytes.Buffer) {
	st.owned = buf
	st.borrowed = nil
}

func (st *autoBatchState[K, V]) setBorrowedPayload(payload []byte) {
	st.owned = nil
	st.borrowed = payload
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
	batch := slices.Grow(ab.batchScratch[:0], n)[:n]
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
	case ab.repeatIDScratchCap > hint*4:
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

func (db *DB[K, V]) buildSetAutoBatchRequest(id K, newVal *V, beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V], cloneValue func(K, *V) *V) (*autoBatchRequest[K, V], error) {
	if newVal == nil {
		return nil, ErrNilValue
	}

	req := &autoBatchRequest[K, V]{
		op:           autoBatchSet,
		id:           id,
		beforeStore:  beforeStore,
		beforeCommit: beforeCommit,
		cloneValue:   cloneValue,
		done:         make(chan error, 1),
	}
	if len(beforeStore) > 0 && cloneValue != nil {
		var err error
		if req.setBaseline, err = cloneBeforeStoreValue(id, newVal, cloneValue); err != nil {
			return nil, err
		}
	} else {
		b := getEncodeBuf()
		if err := db.encode(newVal, b); err != nil {
			releaseEncodeBuf(b)
			return nil, fmt.Errorf("encode: %w", err)
		}
		req.setValue = newVal
		req.setPayload = b
	}
	return req, nil
}

func (db *DB[K, V]) buildPatchAutoBatchRequest(id K, fields []Field, ignoreUnknown bool, beforeProcess []beforeProcessFunc[K, V], beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V]) *autoBatchRequest[K, V] {
	return &autoBatchRequest[K, V]{
		op:                 autoBatchPatch,
		id:                 id,
		patch:              append([]Field(nil), fields...),
		patchIgnoreUnknown: ignoreUnknown,
		beforeProcess:      beforeProcess,
		beforeStore:        beforeStore,
		beforeCommit:       beforeCommit,
	}
}

func (db *DB[K, V]) buildDeleteAutoBatchRequest(id K, beforeCommit []beforeCommitFunc[K, V]) *autoBatchRequest[K, V] {
	return &autoBatchRequest[K, V]{
		op:           autoBatchDelete,
		id:           id,
		beforeCommit: beforeCommit,
	}
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
		releaseAutoBatchRequestResources(reqs)
		if stats {
			db.autoBatcher.fallbackClosed.Add(1)
		}
		return err
	}

	job := &autoBatchJob[K, V]{
		reqs:     reqs,
		isolated: isolated || len(reqs) != 1,
		done:     make(chan error, 1),
	}
	if len(reqs) == 1 {
		reqs[0].done = job.done
	}

	db.autoBatcher.mu.Lock()
	for {
		if err := db.unavailableErr(); err != nil {
			db.autoBatcher.mu.Unlock()
			releaseAutoBatchRequestResources(reqs)
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

	return <-job.done
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

func (db *DB[K, V]) popAutoBatch() []*autoBatchJob[K, V] {
	stats := db.autoBatcher.statsEnabled

	db.autoBatcher.mu.Lock()
	if db.autoBatcher.queueLen() == 0 {
		db.autoBatcher.running = false
		db.autoBatcher.mu.Unlock()
		return nil
	}

	waitDur := time.Duration(0)
	frontLimit := autoBatchFrontLimit(&db.autoBatcher, db.autoBatcher.maxOps)
	if db.autoBatcher.window > 0 && db.autoBatcher.queueLen() < db.autoBatcher.maxOps {
		now := time.Now()
		switch {
		case frontLimit > 1:
			waitDur = db.autoBatcher.window
			keepHot := 4 * db.autoBatcher.window
			if keepHot < 500*time.Microsecond {
				keepHot = 500 * time.Microsecond
			}
			db.autoBatcher.hotUntil = now.Add(keepHot)
		case db.autoBatcher.queueLen() == 1 && now.Before(db.autoBatcher.hotUntil):
			waitDur = db.autoBatcher.window
			if waitDur > 50*time.Microsecond {
				waitDur /= 2
			}
		}
	}

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
	if n > 1 {
		lastByID := db.autoBatcher.acquireRepeatIDMap(n)
		for i := 0; i < n; i++ {
			req := db.autoBatcher.queueAt(i).reqs[0]
			prevIdx, seen := lastByID[req.id]
			if seen {
				prev := db.autoBatcher.queueAt(prevIdx).reqs[0]
				if !(canCoalesceSetDelete(req) && canCoalesceSetDelete(prev)) &&
					!(db.canBatchRepeatedID(req) && db.canBatchRepeatedID(prev)) {
					n = i
					break
				}
			}
			lastByID[req.id] = i
		}
		db.autoBatcher.releaseRepeatIDMap()
	}

	batch := db.autoBatcher.dequeueFrontScratch(n)
	if len(batch) > 1 {
		db.coalesceSetDeleteJobs(batch)
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

func canCoalesceSetDelete[K ~string | ~uint64, V any](req *autoBatchRequest[K, V]) bool {
	if req.coalescedTo != nil || len(req.beforeStore) > 0 || len(req.beforeCommit) > 0 {
		return false
	}
	return req.op == autoBatchSet || req.op == autoBatchDelete
}

func (db *DB[K, V]) canBatchRepeatedID(req *autoBatchRequest[K, V]) bool {
	if req.coalescedTo != nil {
		return false
	}
	switch req.op {
	case autoBatchDelete:
		return true
	case autoBatchPatch:
		if db.hasUnique && (len(req.beforeProcess) > 0 || len(req.beforeStore) > 0) {
			return false
		}
		if !db.hasUnique {
			return true
		}
		return !db.patchTouchesUnique(req.patch)
	default:
		return false
	}
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

func (db *DB[K, V]) coalesceSetDeleteJobs(batch []*autoBatchJob[K, V]) {
	if len(batch) < 2 {
		return
	}
	stats := db.autoBatcher.statsEnabled

	lastByID := db.autoBatcher.acquireRepeatIDMap(len(batch))
	for i := range batch {
		req := batch[i].reqs[0]
		req.coalescedTo = nil

		prevIdx, seen := lastByID[req.id]
		if seen && canCoalesceSetDelete(req) {
			prev := batch[prevIdx].reqs[0]
			if canCoalesceSetDelete(prev) {
				prev.coalescedTo = req
				if stats {
					db.autoBatcher.coalescedSetDelete.Add(1)
				}
			}
		}
		lastByID[req.id] = i
	}
	db.autoBatcher.releaseRepeatIDMap()
}

func (db *DB[K, V]) executeAutoBatchJobs(batch []*autoBatchJob[K, V]) {
	db.recordExecutedAutoBatchStats(len(batch))

	if len(batch) == 1 && batch[0].isolated {
		batch[0].done <- db.executeAutoBatchAtomic(batch[0].reqs)
		return
	}

	reqs := make([]*autoBatchRequest[K, V], 0, len(batch))
	for _, job := range batch {
		if len(job.reqs) != 1 {
			job.done <- fmt.Errorf("internal auto-batch error: mixed grouped request in shared batch")
			releaseAutoBatchRequestResources(job.reqs)
			continue
		}
		req := job.reqs[0]
		req.done = job.done
		reqs = append(reqs, req)
	}
	if len(reqs) == 0 {
		return
	}
	db.executeAutoBatchNoStats(reqs)
}

func (db *DB[K, V]) executeAutoBatch(batch []*autoBatchRequest[K, V]) {
	db.recordExecutedAutoBatchStats(len(batch))
	db.executeAutoBatchNoStats(batch)
}

func (db *DB[K, V]) executeAutoBatchNoStats(batch []*autoBatchRequest[K, V]) {
	for _, req := range batch {
		req.err = nil
	}

	active := make([]*autoBatchRequest[K, V], 0, len(batch))
	for _, req := range batch {
		if req.coalescedTo == nil {
			active = append(active, req)
		}
	}

	for {
		retryWithoutReq, done, fatalErr := db.executeAutoBatchAttempt(active, false)
		if fatalErr != nil {
			db.failAutoBatch(batch, fatalErr)
			return
		}
		if done {
			db.finishAutoBatch(batch)
			return
		}

		out := active[:0]
		removed := false
		for _, req := range active {
			if !removed && req == retryWithoutReq {
				removed = true
				continue
			}
			out = append(out, req)
		}
		active = out
		if !removed {
			db.failAutoBatch(batch, fmt.Errorf("internal auto-batch retry error: failed request not found"))
			return
		}
		if len(active) == 0 {
			db.finishAutoBatch(batch)
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

func (db *DB[K, V]) executeAutoBatchAtomic(batch []*autoBatchRequest[K, V]) error {
	defer releaseAutoBatchRequestResources(batch)
	for _, req := range batch {
		req.err = nil
		req.coalescedTo = nil
	}
	_, done, fatalErr := db.executeAutoBatchAttempt(batch, true)
	if fatalErr != nil {
		return fatalErr
	}
	if !done {
		return fmt.Errorf("internal auto-batch atomic retry error")
	}
	return firstAutoBatchRequestErr(batch)
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

	prepared := make([]autoBatchPrepared[K, V], 0, len(active))
	var buffers []*bytes.Buffer
	defer func() {
		for _, v := range buffers {
			if v != nil {
				releaseEncodeBuf(v)
			}
		}
		clear(buffers)
	}()

	rollbackCreated := func(ops []autoBatchPrepared[K, V]) {
		if !db.strkey {
			return
		}
		for i := range ops {
			op := ops[i]
			if !op.idxNew || op.oldVal != nil || op.newVal == nil {
				continue
			}
			db.rollbackCreatedStrIdx(op.req.id, op.idx)
		}
	}

	states := make(map[K]*autoBatchState[K, V], len(active))

	loadState := func(req *autoBatchRequest[K, V], createIdx bool) (*autoBatchState[K, V], error) {
		st := states[req.id]
		if st == nil {
			st = &autoBatchState[K, V]{
				key: db.keyFromID(req.id),
			}
			if prev := bucket.Get(st.key); prev != nil {
				oldVal, decErr := db.decode(prev)
				if decErr != nil {
					return nil, decErr
				}
				st.current = oldVal
				st.setBorrowedPayload(prev)
			}
			states[req.id] = st
		}
		if createIdx && !st.idxKnown {
			st.idx, st.idxNew = db.idxFromIDWithCreated(req.id)
			st.idxKnown = true
		}
		return st, nil
	}

	ensureIdx := func(req *autoBatchRequest[K, V], st *autoBatchState[K, V], create bool) {
		if st.idxKnown {
			return
		}
		if create {
			st.idx, st.idxNew = db.idxFromIDWithCreated(req.id)
		} else {
			st.idx = db.idxFromID(req.id)
		}
		st.idxKnown = true
	}

	for _, req := range active {
		if req.err != nil {
			continue
		}

		switch req.op {
		case autoBatchSet:
			st, stErr := loadState(req, false)
			if stErr != nil {
				req.err = fmt.Errorf("decode: %w", stErr)
				continue
			}
			oldVal := st.current
			newVal := req.setValue
			payload := req.payloadBytes()
			var owned *bytes.Buffer
			if len(req.beforeStore) > 0 {
				if stats {
					db.autoBatcher.callbackOps.Add(1)
				}
				if req.cloneValue != nil {
					var cloneErr error
					if newVal, cloneErr = cloneBeforeStoreValue(req.id, req.setBaseline, req.cloneValue); cloneErr != nil {
						req.err = cloneErr
						continue
					}
				} else {
					decodedVal, decErr := db.decode(req.payloadBytes())
					if decErr != nil {
						req.err = fmt.Errorf("decode prepared value: %w", decErr)
						continue
					}
					newVal = decodedVal
				}
				if hookErr := runBeforeStoreHooks(req.id, oldVal, newVal, req.beforeStore); hookErr != nil {
					req.err = hookErr
					if stats {
						db.autoBatcher.callbackErrors.Add(1)
					}
					continue
				}

				b := getEncodeBuf()
				if encErr := db.encode(newVal, b); encErr != nil {
					releaseEncodeBuf(b)
					req.err = fmt.Errorf("encode: %w", encErr)
					continue
				}
				owned = b
				buffers = append(buffers, b)
				payload = b.Bytes()
			}
			if !db.transparent {
				ensureIdx(req, st, true)
			}

			prepared = append(prepared, autoBatchPrepared[K, V]{
				req:      req,
				key:      st.key,
				payload:  payload,
				idx:      st.idx,
				idxNew:   st.idxNew,
				oldVal:   oldVal,
				newVal:   newVal,
				modified: db.getModifiedUniqueFields(oldVal, newVal),
			})
			st.current = newVal
			if owned != nil {
				st.setOwnedPayload(owned)
			} else {
				st.setOwnedPayload(req.setPayload)
			}

		case autoBatchPatch:
			st, stErr := loadState(req, false)
			if stErr != nil {
				req.err = fmt.Errorf("failed to decode existing value: %w", stErr)
				continue
			}
			if st.current == nil {
				continue
			}

			oldVal := st.current
			if st.owned == nil && st.borrowed == nil {
				req.err = fmt.Errorf("failed to re-decode value for patching: source payload is empty")
				continue
			}
			newVal, decErr := db.decode(st.payloadBytes())
			if decErr != nil {
				req.err = fmt.Errorf("failed to re-decode value for patching: %w", decErr)
				continue
			}

			if patchErr := db.applyPatch(newVal, req.patch, req.patchIgnoreUnknown); patchErr != nil {
				req.err = fmt.Errorf("failed to apply patch: %w", patchErr)
				continue
			}
			if len(req.beforeProcess) > 0 {
				if hookErr := runBeforeProcessHooks(req.id, newVal, req.beforeProcess); hookErr != nil {
					req.err = hookErr
					continue
				}
			}

			if len(req.beforeStore) > 0 {
				if stats {
					db.autoBatcher.callbackOps.Add(1)
				}
				if hookErr := runBeforeStoreHooks(req.id, oldVal, newVal, req.beforeStore); hookErr != nil {
					req.err = hookErr
					if stats {
						db.autoBatcher.callbackErrors.Add(1)
					}
					continue
				}
			}

			b := getEncodeBuf()
			if encErr := db.encode(newVal, b); encErr != nil {
				releaseEncodeBuf(b)
				req.err = fmt.Errorf("encode: %w", encErr)
				continue
			}
			buffers = append(buffers, b)
			payload := b.Bytes()

			if !db.transparent {
				ensureIdx(req, st, false)
			}

			prepared = append(prepared, autoBatchPrepared[K, V]{
				req:      req,
				key:      st.key,
				payload:  payload,
				idx:      st.idx,
				idxNew:   st.idxNew,
				oldVal:   oldVal,
				newVal:   newVal,
				modified: db.getModifiedUniqueFields(oldVal, newVal),
			})
			st.current = newVal
			st.setOwnedPayload(b)

		case autoBatchDelete:
			st, stErr := loadState(req, false)
			if stErr != nil {
				req.err = fmt.Errorf("decode: %w", stErr)
				continue
			}
			if st.current == nil {
				continue
			}
			oldVal := st.current
			if !db.transparent {
				ensureIdx(req, st, false)
			}

			prepared = append(prepared, autoBatchPrepared[K, V]{
				req:      req,
				key:      st.key,
				idx:      st.idx,
				idxNew:   st.idxNew,
				oldVal:   oldVal,
				newVal:   nil,
				modified: db.getModifiedUniqueFields(oldVal, nil),
			})
			st.current = nil
			st.owned = nil
			st.borrowed = nil
		}
	}

	if atomicAll {
		if reqErr := firstAutoBatchRequestErr(active); reqErr != nil {
			rollbackCreated(prepared)
			return nil, true, reqErr
		}
	}

	if len(prepared) == 0 {
		return nil, true, nil
	}

	db.mu.Lock()
	if err = db.unavailableErr(); err != nil {
		rollbackCreated(prepared)
		db.mu.Unlock()
		for _, op := range prepared {
			if op.req.err == nil {
				op.req.err = err
			}
		}
		return nil, true, nil
	}

	accepted := prepared
	var atomicErr error
	if db.hasUnique {
		if atomicAll {
			idxs := make([]uint64, len(prepared))
			oldVals := make([]*V, len(prepared))
			newVals := make([]*V, len(prepared))
			modified := make([][]string, len(prepared))
			for i := range prepared {
				op := prepared[i]
				idxs[i] = op.idx
				oldVals[i] = op.oldVal
				newVals[i] = op.newVal
				modified[i] = op.modified
			}
			if uerr := db.checkUniqueOnWriteMulti(idxs, oldVals, newVals, modified); uerr != nil {
				if stats {
					db.autoBatcher.uniqueRejected.Add(1)
				}
				for _, op := range prepared {
					op.req.err = uerr
				}
				atomicErr = uerr
			}
		} else {
			accepted = make([]autoBatchPrepared[K, V], 0, len(prepared))
			uniqueState := db.newUniqueBatchCheckState()
			defer db.releaseUniqueBatchCheckState(uniqueState)
			for _, op := range prepared {
				if uerr := db.checkUniqueBatchAppend(uniqueState, op.idx, op.oldVal, op.newVal, op.modified); uerr != nil {
					op.req.err = uerr
					if stats {
						db.autoBatcher.uniqueRejected.Add(1)
					}
					db.rollbackCreatedStrIdxIfNeeded(op)
					continue
				}
				accepted = append(accepted, op)
			}
		}
	}
	if atomicErr != nil {
		rollbackCreated(prepared)
		db.mu.Unlock()
		return nil, true, atomicErr
	}

	if len(accepted) == 0 {
		db.mu.Unlock()
		return nil, true, nil
	}

	var fatalErr error
	var callbackFailedReq *autoBatchRequest[K, V]

	for _, op := range accepted {
		switch op.req.op {
		case autoBatchSet, autoBatchPatch:
			if err = bucket.Put(op.key, op.payload); err != nil {
				fatalErr = fmt.Errorf("put: %w", err)
			}
		case autoBatchDelete:
			if err = bucket.Delete(op.key); err != nil {
				fatalErr = fmt.Errorf("delete: %w", err)
			}
		default:
			fatalErr = fmt.Errorf("unknown auto-batch op: %v", op.req.op)
		}
		if fatalErr != nil {
			if stats {
				db.autoBatcher.txOpErrors.Add(1)
			}
			break
		}

		if len(op.req.beforeCommit) == 0 {
			continue
		}

		if stats {
			if len(op.req.beforeStore) == 0 {
				db.autoBatcher.callbackOps.Add(1)
			}
		}
		if cbErr := runBeforeCommitHooks(tx, op.req.id, op.oldVal, op.newVal, op.req.beforeCommit); cbErr != nil {
			op.req.err = cbErr
			callbackFailedReq = op.req
			if stats {
				db.autoBatcher.callbackErrors.Add(1)
			}
			fatalErr = cbErr
		}
		if fatalErr != nil {
			if stats {
				db.autoBatcher.txOpErrors.Add(1)
			}
			break
		}
	}

	if fatalErr != nil {
		rollbackCreated(accepted)
		db.mu.Unlock()
		if callbackFailedReq != nil && !atomicAll {
			return callbackFailedReq, false, nil
		}
		return nil, true, fatalErr
	}

	if db.transparent {
		if _, err = bucket.NextSequence(); err != nil {
			if stats {
				db.autoBatcher.txOpErrors.Add(1)
			}
			rollbackCreated(accepted)
			db.mu.Unlock()
			return nil, true, fmt.Errorf("advance bucket sequence: %w", err)
		}
		if err = db.commit(tx, opName); err != nil {
			if stats {
				db.autoBatcher.txCommitErrors.Add(1)
			}
			db.mu.Unlock()
			for _, op := range accepted {
				if op.req.err == nil {
					op.req.err = err
				}
			}
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
		rollbackCreated(accepted)
		db.mu.Unlock()
		return nil, true, fmt.Errorf("advance bucket sequence: %w", err)
	}

	snap := db.buildPreparedSnapshotNoLock(seq, accepted)
	db.stageSnapshot(snap)

	if err = db.commit(tx, opName); err != nil {
		if stats {
			db.autoBatcher.txCommitErrors.Add(1)
		}
		db.dropStagedSnapshot(seq)
		rollbackCreated(accepted)
		db.mu.Unlock()
		for _, op := range accepted {
			if op.req.err == nil {
				op.req.err = err
			}
		}
		return nil, true, nil
	}

	err = db.publishAfterCommitLocked(seq, opName, func() {
		db.finishSnapshotPublishNoLock(snap)
	})
	if err != nil {
		db.mu.Unlock()
		for _, op := range accepted {
			if op.req.err == nil {
				op.req.err = err
			}
		}
		return nil, true, nil
	}
	db.mu.Unlock()

	return nil, true, nil
}

func (db *DB[K, V]) failAutoBatchJobs(batch []*autoBatchJob[K, V], err error) {
	for _, job := range batch {
		for _, req := range job.reqs {
			if req != nil && req.err == nil {
				req.err = err
			}
		}
		job.done <- err
		releaseAutoBatchRequestResources(job.reqs)
	}
}

func (db *DB[K, V]) failAutoBatch(batch []*autoBatchRequest[K, V], err error) {
	for _, req := range batch {
		if req.err == nil {
			req.err = err
		}
	}
	db.finishAutoBatch(batch)
}

func (db *DB[K, V]) finishAutoBatch(batch []*autoBatchRequest[K, V]) {
	for _, req := range batch {
		if req.coalescedTo != nil {
			target := req.coalescedTo
			for target.coalescedTo != nil {
				target = target.coalescedTo
			}
			req.err = target.err
		}
		req.done <- req.err
	}
	releaseAutoBatchRequestResources(batch)
}
