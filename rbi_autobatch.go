package rbi

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type combineOp uint8

const (
	combineSet combineOp = iota + 1
	combinePatch
	combineDelete
)

type combineRequest[K ~string | ~uint64, V any] struct {
	op combineOp
	id K

	setValue    *V
	setBaseline *V
	setPayload  []byte

	patch              []Field
	patchIgnoreUnknown bool

	beforeProcess []beforeProcessFunc[K, V]
	beforeStore   []beforeStoreFunc[K, V]
	beforeCommit  []beforeCommitFunc[K, V]
	cloneValue    func(K, *V) *V

	coalescedTo *combineRequest[K, V]

	err  error
	done chan error
}

type combinedBatchPrepared[K ~string | ~uint64, V any] struct {
	req *combineRequest[K, V]

	key      []byte
	payload  []byte
	idx      uint64
	idxNew   bool
	oldVal   *V
	newVal   *V
	modified []string
}

type combinedBatchState[K ~string | ~uint64, V any] struct {
	key []byte

	idx      uint64
	idxKnown bool
	idxNew   bool

	current        *V
	currentPayload []byte
}

func rollbackCreatedStrIdxIfNeeded[K ~string | ~uint64, V any](db *DB[K, V], op combinedBatchPrepared[K, V]) {
	if db == nil || !db.strkey {
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

func (db *DB[K, V]) tryQueueSetCombine(id K, newVal *V, beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V], cloneValue func(K, *V) *V) (error, bool) {
	if !db.combiner.enabled {
		db.combiner.fallbackDisabled.Add(1)
		return nil, false
	}
	if newVal == nil {
		return ErrNilValue, true
	}
	if err := db.unavailableErr(); err != nil {
		db.combiner.fallbackClosed.Add(1)
		return err, true
	}

	req := &combineRequest[K, V]{
		op:           combineSet,
		id:           id,
		beforeStore:  beforeStore,
		beforeCommit: beforeCommit,
		cloneValue:   cloneValue,
		done:         make(chan error, 1),
	}
	if len(beforeStore) > 0 && cloneValue != nil {
		var err error
		if req.setBaseline, err = cloneBeforeStoreValue(id, newVal, cloneValue); err != nil {
			return err, true
		}
	} else {
		b := getEncodeBuf()
		defer releaseEncodeBuf(b)
		if err := db.encode(newVal, b); err != nil {
			return fmt.Errorf("encode: %w", err), true
		}
		req.setValue = newVal
		req.setPayload = append([]byte(nil), b.Bytes()...)
	}
	return db.submitCombinedBatch(req)
}

func (db *DB[K, V]) tryQueuePatchCombine(id K, fields []Field, ignoreUnknown bool, beforeProcess []beforeProcessFunc[K, V], beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V]) (error, bool) {
	if !db.combiner.enabled {
		db.combiner.fallbackDisabled.Add(1)
		return nil, false
	}
	if err := db.unavailableErr(); err != nil {
		db.combiner.fallbackClosed.Add(1)
		return err, true
	}

	req := &combineRequest[K, V]{
		op:                 combinePatch,
		id:                 id,
		patch:              append([]Field(nil), fields...),
		patchIgnoreUnknown: ignoreUnknown,
		beforeProcess:      beforeProcess,
		beforeStore:        beforeStore,
		beforeCommit:       beforeCommit,
		done:               make(chan error, 1),
	}
	return db.submitCombinedBatch(req)
}

func (db *DB[K, V]) tryQueueDeleteCombine(id K, beforeCommit []beforeCommitFunc[K, V]) (error, bool) {
	if !db.combiner.enabled {
		db.combiner.fallbackDisabled.Add(1)
		return nil, false
	}
	if err := db.unavailableErr(); err != nil {
		db.combiner.fallbackClosed.Add(1)
		return err, true
	}

	req := &combineRequest[K, V]{
		op:           combineDelete,
		id:           id,
		beforeCommit: beforeCommit,
		done:         make(chan error, 1),
	}
	return db.submitCombinedBatch(req)
}

func (db *DB[K, V]) submitCombinedBatch(req *combineRequest[K, V]) (error, bool) {
	if !db.combiner.enabled {
		db.combiner.fallbackDisabled.Add(1)
		return nil, false
	}
	db.combiner.submitted.Add(1)

	startRunner := false
	db.combiner.mu.Lock()
	if !db.combiner.enabled {
		db.combiner.mu.Unlock()
		db.combiner.fallbackDisabled.Add(1)
		return nil, false
	}
	if db.combiner.maxQ > 0 && len(db.combiner.queue) >= db.combiner.maxQ {
		db.combiner.mu.Unlock()
		db.combiner.fallbackQueueFull.Add(1)
		return nil, false
	}

	db.combiner.queue = append(db.combiner.queue, req)
	db.combiner.enqueued.Add(1)
	atomicSetMax(&db.combiner.queueHighWater, uint64(len(db.combiner.queue)))

	if !db.combiner.running {
		db.combiner.running = true
		startRunner = true
	}
	db.combiner.mu.Unlock()

	if startRunner {
		go db.runCombinerLoop()
	}

	return <-req.done, true
}

func (db *DB[K, V]) runCombinerLoop() {
	for {
		batch := db.popCombinedBatch()
		if len(batch) == 0 {
			return
		}
		if err := db.unavailableErr(); err != nil {
			db.failCombinedBatch(batch, err)
			continue
		}
		db.executeCombinedBatch(batch)
	}
}

func (db *DB[K, V]) popCombinedBatch() []*combineRequest[K, V] {

	db.combiner.mu.Lock()
	if len(db.combiner.queue) == 0 {
		db.combiner.running = false
		db.combiner.mu.Unlock()
		return nil
	}

	waitDur := time.Duration(0)
	if db.combiner.window > 0 && len(db.combiner.queue) < db.combiner.maxOps {
		now := time.Now()
		switch {
		case len(db.combiner.queue) > 1:
			waitDur = db.combiner.window
			keepHot := 4 * db.combiner.window
			if keepHot < 500*time.Microsecond {
				keepHot = 500 * time.Microsecond
			}
			db.combiner.hotUntil = now.Add(keepHot)
		case now.Before(db.combiner.hotUntil):
			waitDur = db.combiner.window
			if waitDur > 50*time.Microsecond {
				waitDur /= 2
			}
		}
	}

	if waitDur > 0 {
		db.combiner.mu.Unlock()
		start := time.Now()
		time.Sleep(waitDur)
		db.combiner.coalesceWaits.Add(1)
		db.combiner.coalesceWaitNanos.Add(uint64(time.Since(start)))
		db.combiner.mu.Lock()
		if len(db.combiner.queue) == 0 {
			db.combiner.running = false
			db.combiner.mu.Unlock()
			return nil
		}
	}

	n := db.combiner.maxOps
	if n <= 0 || n > len(db.combiner.queue) {
		n = len(db.combiner.queue)
	}

	if n > 1 {
		lastByID := make(map[K]int, n)
		for i := 0; i < n; i++ {
			req := db.combiner.queue[i]
			prevIdx, seen := lastByID[req.id]
			if seen {
				prev := db.combiner.queue[prevIdx]
				if !(canCoalesceSetDelete(req) && canCoalesceSetDelete(prev)) &&
					!(db.canBatchRepeatedID(req) && db.canBatchRepeatedID(prev)) {
					n = i
					break
				}
			}
			lastByID[req.id] = i
		}
	}

	batch := append([]*combineRequest[K, V](nil), db.combiner.queue[:n]...)
	if len(batch) > 1 {
		db.coalesceSetDeleteBatch(batch)
	}
	db.combiner.queue = append(db.combiner.queue[:0], db.combiner.queue[n:]...)
	db.combiner.dequeued.Add(uint64(len(batch)))
	db.combiner.mu.Unlock()
	return batch
}

func canCoalesceSetDelete[K ~string | ~uint64, V any](req *combineRequest[K, V]) bool {
	if req == nil || req.coalescedTo != nil || hasExecHooks(req.beforeStore, req.beforeCommit) {
		return false
	}
	return req.op == combineSet || req.op == combineDelete
}

func (db *DB[K, V]) canBatchRepeatedID(req *combineRequest[K, V]) bool {
	if req == nil || req.coalescedTo != nil {
		return false
	}
	switch req.op {
	case combineDelete:
		return true
	case combinePatch:
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

func (db *DB[K, V]) coalesceSetDeleteBatch(batch []*combineRequest[K, V]) {
	if len(batch) < 2 {
		return
	}

	lastByID := make(map[K]int, len(batch))
	for i := range batch {
		req := batch[i]
		req.coalescedTo = nil

		prevIdx, seen := lastByID[req.id]
		if seen && canCoalesceSetDelete(req) {
			prev := batch[prevIdx]
			if canCoalesceSetDelete(prev) {
				prev.coalescedTo = req
				db.combiner.coalescedSetDelete.Add(1)
			}
		}
		lastByID[req.id] = i
	}
}

func (db *DB[K, V]) executeCombinedBatch(batch []*combineRequest[K, V]) {
	db.combiner.batches.Add(1)
	if len(batch) > 1 {
		db.combiner.combinedBatches.Add(1)
		db.combiner.combinedOps.Add(uint64(len(batch)))
	}
	atomicSetMax(&db.combiner.maxBatchSeen, uint64(len(batch)))

	for _, req := range batch {
		req.err = nil
	}

	active := make([]*combineRequest[K, V], 0, len(batch))
	for _, req := range batch {
		if req.coalescedTo == nil {
			active = append(active, req)
		}
	}

	for {
		retryWithoutReq, done, fatalErr := db.executeCombinedBatchAttempt(active)
		if fatalErr != nil {
			db.failCombinedBatch(batch, fatalErr)
			return
		}
		if done {
			db.finishCombinedBatch(batch)
			return
		}

		prevN := len(active)
		active = removeCombineRequestByPtr(active, retryWithoutReq)
		if len(active) == prevN {
			db.failCombinedBatch(batch, fmt.Errorf("internal batch retry error: failed request not found"))
			return
		}
		if len(active) == 0 {
			db.finishCombinedBatch(batch)
			return
		}
		resetRetryableBatchErrors(active)
	}
}

func (db *DB[K, V]) executeCombinedBatchAttempt(active []*combineRequest[K, V]) (*combineRequest[K, V], bool, error) {
	tx, err := db.bolt.Begin(true)
	if err != nil {
		db.combiner.txBeginErrors.Add(1)
		return nil, true, fmt.Errorf("tx error: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			rollback(tx)
		}
	}()

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		db.combiner.txOpErrors.Add(1)
		return nil, true, fmt.Errorf("bucket does not exist")
	}

	bucket.FillPercent = db.options.BucketFillPercent

	prepared := make([]combinedBatchPrepared[K, V], 0, len(active))

	rollbackCreated := func(ops []combinedBatchPrepared[K, V]) {
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

	states := make(map[K]*combinedBatchState[K, V], len(active))

	loadState := func(req *combineRequest[K, V], createIdx bool) (*combinedBatchState[K, V], error) {
		st := states[req.id]
		if st == nil {
			st = &combinedBatchState[K, V]{
				key: db.keyFromID(req.id),
			}
			if prev := bucket.Get(st.key); prev != nil {
				oldVal, decErr := db.decode(prev)
				if decErr != nil {
					return nil, decErr
				}
				st.current = oldVal
				st.currentPayload = prev
			}
			states[req.id] = st
		}
		if createIdx && !st.idxKnown {
			st.idx, st.idxNew = db.idxFromIDWithCreated(req.id)
			st.idxKnown = true
		}
		return st, nil
	}

	ensureIdx := func(req *combineRequest[K, V], st *combinedBatchState[K, V], create bool) {
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
		case combineSet:
			st, stErr := loadState(req, false)
			if stErr != nil {
				req.err = fmt.Errorf("decode: %w", stErr)
				continue
			}
			oldVal := st.current
			newVal := req.setValue
			payload := req.setPayload
			if len(req.beforeStore) > 0 {
				db.combiner.callbackOps.Add(1)
				if req.cloneValue != nil {
					var cloneErr error
					if newVal, cloneErr = cloneBeforeStoreValue(req.id, req.setBaseline, req.cloneValue); cloneErr != nil {
						req.err = cloneErr
						continue
					}
				} else {
					decodedVal, decErr := db.decode(req.setPayload)
					if decErr != nil {
						req.err = fmt.Errorf("decode prepared value: %w", decErr)
						continue
					}
					newVal = decodedVal
				}
				if hookErr := runBeforeStoreHooks(req.id, oldVal, newVal, req.beforeStore); hookErr != nil {
					req.err = hookErr
					db.combiner.callbackErrors.Add(1)
					continue
				}

				b := getEncodeBuf()
				if encErr := db.encode(newVal, b); encErr != nil {
					releaseEncodeBuf(b)
					req.err = fmt.Errorf("encode: %w", encErr)
					continue
				}
				payload = append([]byte(nil), b.Bytes()...)
				releaseEncodeBuf(b)
			}
			ensureIdx(req, st, true)

			prepared = append(prepared, combinedBatchPrepared[K, V]{
				req:      req,
				key:      st.key,
				payload:  payload,
				idx:      st.idx,
				idxNew:   st.idxNew,
				oldVal:   oldVal,
				newVal:   newVal,
				modified: db.getModifiedIndexedFields(oldVal, newVal),
			})
			st.current = newVal
			st.currentPayload = payload

		case combinePatch:
			st, stErr := loadState(req, false)
			if stErr != nil {
				req.err = fmt.Errorf("failed to decode existing value: %w", stErr)
				continue
			}
			if st.current == nil {
				continue
			}

			oldVal := st.current
			if st.currentPayload == nil {
				req.err = fmt.Errorf("failed to re-decode value for patching: source payload is empty")
				continue
			}
			newVal, decErr := db.decode(st.currentPayload)
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
				db.combiner.callbackOps.Add(1)
				if hookErr := runBeforeStoreHooks(req.id, oldVal, newVal, req.beforeStore); hookErr != nil {
					req.err = hookErr
					db.combiner.callbackErrors.Add(1)
					continue
				}
			}

			b := getEncodeBuf()
			if encErr := db.encode(newVal, b); encErr != nil {
				releaseEncodeBuf(b)
				req.err = fmt.Errorf("encode: %w", encErr)
				continue
			}
			payload := append([]byte(nil), b.Bytes()...)
			releaseEncodeBuf(b)
			ensureIdx(req, st, false)

			prepared = append(prepared, combinedBatchPrepared[K, V]{
				req:      req,
				key:      st.key,
				payload:  payload,
				idx:      st.idx,
				idxNew:   st.idxNew,
				oldVal:   oldVal,
				newVal:   newVal,
				modified: db.getModifiedIndexedFields(oldVal, newVal),
			})
			st.current = newVal
			st.currentPayload = payload

		case combineDelete:
			st, stErr := loadState(req, false)
			if stErr != nil {
				req.err = fmt.Errorf("decode: %w", stErr)
				continue
			}
			if st.current == nil {
				continue
			}
			oldVal := st.current
			ensureIdx(req, st, false)

			prepared = append(prepared, combinedBatchPrepared[K, V]{
				req:      req,
				key:      st.key,
				idx:      st.idx,
				idxNew:   st.idxNew,
				oldVal:   oldVal,
				newVal:   nil,
				modified: db.getModifiedIndexedFields(oldVal, nil),
			})
			st.current = nil
			st.currentPayload = nil
		}
	}

	if len(prepared) == 0 {
		return nil, true, nil
	}

	preparedIdxs := make([]uint64, len(prepared))
	preparedOldVals := make([]*V, len(prepared))
	preparedNewVals := make([]*V, len(prepared))
	preparedModified := make([][]string, len(prepared))
	for i, op := range prepared {
		preparedIdxs[i] = op.idx
		preparedOldVals[i] = op.oldVal
		preparedNewVals[i] = op.newVal
		preparedModified[i] = op.modified
	}
	preparedDelta := db.prepareSnapshotWriteDeltaBatch(preparedIdxs, preparedOldVals, preparedNewVals, preparedModified)
	preparedDeltaOwned := true
	defer func() {
		if preparedDeltaOwned {
			preparedDelta.release()
		}
	}()

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
	if db.hasUnique {
		accepted = make([]combinedBatchPrepared[K, V], 0, len(prepared))
		uniqueState := db.newUniqueBatchCheckState()
		defer db.releaseUniqueBatchCheckState(uniqueState)
		for _, op := range prepared {
			if uerr := db.checkUniqueBatchAppend(uniqueState, op.idx, op.oldVal, op.newVal, op.modified); uerr != nil {
				op.req.err = uerr
				db.combiner.uniqueRejected.Add(1)
				rollbackCreatedStrIdxIfNeeded(db, op)
				continue
			}
			accepted = append(accepted, op)
		}
	}

	if len(accepted) == 0 {
		db.mu.Unlock()
		return nil, true, nil
	}

	var fatalErr error
	var callbackFailedReq *combineRequest[K, V]
	for _, op := range accepted {
		switch op.req.op {
		case combineSet, combinePatch:
			if err = bucket.Put(op.key, op.payload); err != nil {
				fatalErr = fmt.Errorf("put: %w", err)
			}
		case combineDelete:
			if err = bucket.Delete(op.key); err != nil {
				fatalErr = fmt.Errorf("delete: %w", err)
			}
		default:
			fatalErr = fmt.Errorf("unknown combine op: %v", op.req.op)
		}
		if fatalErr != nil {
			db.combiner.txOpErrors.Add(1)
			break
		}

		if len(op.req.beforeCommit) == 0 {
			continue
		}
		if len(op.req.beforeStore) == 0 {
			db.combiner.callbackOps.Add(1)
		}
		if cbErr := runBeforeCommitHooks(tx, op.req.id, op.oldVal, op.newVal, op.req.beforeCommit); cbErr != nil {
			op.req.err = cbErr
			callbackFailedReq = op.req
			db.combiner.callbackErrors.Add(1)
			fatalErr = cbErr
		}
		if fatalErr != nil {
			db.combiner.txOpErrors.Add(1)
			break
		}
	}

	if fatalErr != nil {
		rollbackCreated(accepted)
		db.mu.Unlock()
		if callbackFailedReq != nil {
			return callbackFailedReq, false, nil
		}
		return nil, true, fatalErr
	}
	if err = advanceBucketSequence(bucket); err != nil {
		db.combiner.txOpErrors.Add(1)
		rollbackCreated(accepted)
		db.mu.Unlock()
		return nil, true, err
	}

	idxs := make([]uint64, len(accepted))
	oldVals := make([]*V, len(accepted))
	newVals := make([]*V, len(accepted))
	modified := make([][]string, len(accepted))
	for i, op := range accepted {
		idxs[i] = op.idx
		oldVals[i] = op.oldVal
		newVals[i] = op.newVal
		modified[i] = op.modified
	}

	txID := uint64(tx.ID())
	db.markPending(txID)
	if err = db.commit(tx, "batch"); err != nil {
		db.combiner.txCommitErrors.Add(1)
		db.clearPending(txID)
		rollbackCreated(accepted)
		db.mu.Unlock()
		for _, op := range accepted {
			if op.req.err == nil {
				op.req.err = err
			}
		}
		return nil, true, nil
	}
	committed = true
	if len(accepted) == len(prepared) {
		err = db.publishAfterCommitLocked(txID, "batch", func() {
			db.publishPreparedSnapshotWithAccumDeltaNoLock(txID, &preparedDelta)
		})
		if err == nil {
			preparedDeltaOwned = false
		}
	} else {
		err = db.publishAfterCommitLocked(txID, "batch", func() {
			db.publishWriteDeltaBatch(txID, idxs, oldVals, newVals, modified)
		})
	}
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

func removeCombineRequestByPtr[K ~string | ~uint64, V any](reqs []*combineRequest[K, V], victim *combineRequest[K, V]) []*combineRequest[K, V] {
	if victim == nil {
		return reqs
	}
	out := reqs[:0]
	removed := false
	for _, req := range reqs {
		if !removed && req == victim {
			removed = true
			continue
		}
		out = append(out, req)
	}
	return out
}

func resetRetryableBatchErrors[K ~string | ~uint64, V any](reqs []*combineRequest[K, V]) {
	for _, req := range reqs {
		if req == nil || req.err == nil {
			continue
		}
		if errors.Is(req.err, ErrUniqueViolation) {
			req.err = nil
		}
	}
}

func (db *DB[K, V]) failCombinedBatch(batch []*combineRequest[K, V], err error) {
	for _, req := range batch {
		if req.err == nil {
			req.err = err
		}
	}
	db.finishCombinedBatch(batch)
}

func (db *DB[K, V]) finishCombinedBatch(batch []*combineRequest[K, V]) {
	for _, req := range batch {
		if req.coalescedTo != nil {
			target := req.coalescedTo
			for target != nil && target.coalescedTo != nil {
				target = target.coalescedTo
			}
			if target == nil {
				req.err = nil
			} else {
				req.err = target.err
			}
		}
		req.done <- req.err
	}
}
