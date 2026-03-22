package rbi

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
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
	setPayload  []byte

	patch              []Field
	patchIgnoreUnknown bool

	beforeProcess []beforeProcessFunc[K, V]
	beforeStore   []beforeStoreFunc[K, V]
	beforeCommit  []beforeCommitFunc[K, V]
	cloneValue    func(K, *V) *V

	coalescedTo *autoBatchRequest[K, V]

	err  error
	done chan error
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

	current        *V
	currentPayload []byte
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

func (db *DB[K, V]) trySetViaAutoBatch(id K, newVal *V, beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V], cloneValue func(K, *V) *V) (error, bool) {
	if !db.autoBatcher.enabled {
		if db.autoBatcher.statsEnabled {
			db.autoBatcher.fallbackDisabled.Add(1)
		}
		return nil, false
	}
	if newVal == nil {
		return ErrNilValue, true
	}
	if err := db.unavailableErr(); err != nil {
		if db.autoBatcher.statsEnabled {
			db.autoBatcher.fallbackClosed.Add(1)
		}
		return err, true
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
	return db.submitAutoBatch(req)
}

func (db *DB[K, V]) tryPatchViaAutoBatch(id K, fields []Field, ignoreUnknown bool, beforeProcess []beforeProcessFunc[K, V], beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V]) (error, bool) {
	if !db.autoBatcher.enabled {
		if db.autoBatcher.statsEnabled {
			db.autoBatcher.fallbackDisabled.Add(1)
		}
		return nil, false
	}
	if err := db.unavailableErr(); err != nil {
		if db.autoBatcher.statsEnabled {
			db.autoBatcher.fallbackClosed.Add(1)
		}
		return err, true
	}

	req := &autoBatchRequest[K, V]{
		op:                 autoBatchPatch,
		id:                 id,
		patch:              append([]Field(nil), fields...),
		patchIgnoreUnknown: ignoreUnknown,
		beforeProcess:      beforeProcess,
		beforeStore:        beforeStore,
		beforeCommit:       beforeCommit,
		done:               make(chan error, 1),
	}
	return db.submitAutoBatch(req)
}

func (db *DB[K, V]) tryDeleteViaAutoBatch(id K, beforeCommit []beforeCommitFunc[K, V]) (error, bool) {
	if !db.autoBatcher.enabled {
		if db.autoBatcher.statsEnabled {
			db.autoBatcher.fallbackDisabled.Add(1)
		}
		return nil, false
	}
	if err := db.unavailableErr(); err != nil {
		if db.autoBatcher.statsEnabled {
			db.autoBatcher.fallbackClosed.Add(1)
		}
		return err, true
	}

	req := &autoBatchRequest[K, V]{
		op:           autoBatchDelete,
		id:           id,
		beforeCommit: beforeCommit,
		done:         make(chan error, 1),
	}
	return db.submitAutoBatch(req)
}

func (db *DB[K, V]) submitAutoBatch(req *autoBatchRequest[K, V]) (error, bool) {
	stats := db.autoBatcher.statsEnabled
	if stats {
		db.autoBatcher.submitted.Add(1)
	}

	startRunner := false
	db.autoBatcher.mu.Lock()
	if db.autoBatcher.maxQ > 0 && len(db.autoBatcher.queue) >= db.autoBatcher.maxQ {
		db.autoBatcher.mu.Unlock()
		if stats {
			db.autoBatcher.fallbackQueueFull.Add(1)
		}
		return nil, false
	}

	db.autoBatcher.queue = append(db.autoBatcher.queue, req)
	if stats {
		db.autoBatcher.enqueued.Add(1)
		atomicSetMax(&db.autoBatcher.queueHighWater, uint64(len(db.autoBatcher.queue)))
	}

	if !db.autoBatcher.running {
		db.autoBatcher.running = true
		startRunner = true
	}
	db.autoBatcher.mu.Unlock()

	if startRunner {
		go db.runAutoBatcherLoop()
	}

	return <-req.done, true
}

func (db *DB[K, V]) runAutoBatcherLoop() {
	for {
		batch := db.popAutoBatch()
		if len(batch) == 0 {
			return
		}
		if err := db.unavailableErr(); err != nil {
			db.failAutoBatch(batch, err)
			continue
		}
		db.executeAutoBatch(batch)
	}
}

func (db *DB[K, V]) popAutoBatch() []*autoBatchRequest[K, V] {
	stats := db.autoBatcher.statsEnabled

	db.autoBatcher.mu.Lock()
	if len(db.autoBatcher.queue) == 0 {
		db.autoBatcher.running = false
		db.autoBatcher.mu.Unlock()
		return nil
	}

	waitDur := time.Duration(0)
	if db.autoBatcher.window > 0 && len(db.autoBatcher.queue) < db.autoBatcher.maxOps {
		now := time.Now()
		switch {
		case len(db.autoBatcher.queue) > 1:
			waitDur = db.autoBatcher.window
			keepHot := 4 * db.autoBatcher.window
			if keepHot < 500*time.Microsecond {
				keepHot = 500 * time.Microsecond
			}
			db.autoBatcher.hotUntil = now.Add(keepHot)
		case now.Before(db.autoBatcher.hotUntil):
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
		if len(db.autoBatcher.queue) == 0 {
			db.autoBatcher.running = false
			db.autoBatcher.mu.Unlock()
			return nil
		}
	}

	n := db.autoBatcher.maxOps
	if n <= 0 || n > len(db.autoBatcher.queue) {
		n = len(db.autoBatcher.queue)
	}

	if n > 1 {
		lastByID := make(map[K]int, n)
		for i := 0; i < n; i++ {
			req := db.autoBatcher.queue[i]
			prevIdx, seen := lastByID[req.id]
			if seen {
				prev := db.autoBatcher.queue[prevIdx]
				if !(canCoalesceSetDelete(req) && canCoalesceSetDelete(prev)) &&
					!(db.canBatchRepeatedID(req) && db.canBatchRepeatedID(prev)) {
					n = i
					break
				}
			}
			lastByID[req.id] = i
		}
	}

	batch := append([]*autoBatchRequest[K, V](nil), db.autoBatcher.queue[:n]...)
	if len(batch) > 1 {
		db.coalesceSetDeleteBatch(batch)
	}
	db.autoBatcher.queue = append(db.autoBatcher.queue[:0], db.autoBatcher.queue[n:]...)
	if stats {
		db.autoBatcher.dequeued.Add(uint64(len(batch)))
	}
	db.autoBatcher.mu.Unlock()
	return batch
}

func canCoalesceSetDelete[K ~string | ~uint64, V any](req *autoBatchRequest[K, V]) bool {
	if req.coalescedTo != nil || hasExecHooks(req.beforeStore, req.beforeCommit) {
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

func (db *DB[K, V]) coalesceSetDeleteBatch(batch []*autoBatchRequest[K, V]) {
	if len(batch) < 2 {
		return
	}
	stats := db.autoBatcher.statsEnabled

	lastByID := make(map[K]int, len(batch))
	for i := range batch {
		req := batch[i]
		req.coalescedTo = nil

		prevIdx, seen := lastByID[req.id]
		if seen && canCoalesceSetDelete(req) {
			prev := batch[prevIdx]
			if canCoalesceSetDelete(prev) {
				prev.coalescedTo = req
				if stats {
					db.autoBatcher.coalescedSetDelete.Add(1)
				}
			}
		}
		lastByID[req.id] = i
	}
}

func (db *DB[K, V]) executeAutoBatch(batch []*autoBatchRequest[K, V]) {
	stats := db.autoBatcher.statsEnabled
	if stats {
		db.autoBatcher.executedBatches.Add(1)
		if len(batch) > 1 {
			db.autoBatcher.multiReqBatches.Add(1)
			db.autoBatcher.multiReqOps.Add(uint64(len(batch)))
		}
		switch {
		case len(batch) <= 1:
			db.autoBatcher.batchSize1.Add(1)
		case len(batch) <= 4:
			db.autoBatcher.batchSize2To4.Add(1)
		case len(batch) <= 8:
			db.autoBatcher.batchSize5To8.Add(1)
		default:
			db.autoBatcher.batchSize9Plus.Add(1)
		}
		atomicSetMax(&db.autoBatcher.maxBatchSeen, uint64(len(batch)))
	}

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
		retryWithoutReq, done, fatalErr := db.executeAutoBatchAttempt(active)
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

func (db *DB[K, V]) executeAutoBatchAttempt(active []*autoBatchRequest[K, V]) (*autoBatchRequest[K, V], bool, error) {
	stats := db.autoBatcher.statsEnabled

	tx, err := db.bolt.Begin(true)
	if err != nil {
		if stats {
			db.autoBatcher.txBeginErrors.Add(1)
		}
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
		if stats {
			db.autoBatcher.txOpErrors.Add(1)
		}
		return nil, true, fmt.Errorf("bucket does not exist")
	}

	bucket.FillPercent = db.options.BucketFillPercent

	prepared := make([]autoBatchPrepared[K, V], 0, len(active))

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
			payload := req.setPayload
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
					decodedVal, decErr := db.decode(req.setPayload)
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
				payload = append([]byte(nil), b.Bytes()...)
				releaseEncodeBuf(b)
			}
			ensureIdx(req, st, true)

			prepared = append(prepared, autoBatchPrepared[K, V]{
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
			payload := append([]byte(nil), b.Bytes()...)
			releaseEncodeBuf(b)
			ensureIdx(req, st, false)

			prepared = append(prepared, autoBatchPrepared[K, V]{
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
			ensureIdx(req, st, false)

			prepared = append(prepared, autoBatchPrepared[K, V]{
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

	preparedDelta := db.prepareSnapshotWriteDeltaPrepared(prepared)
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
		if len(op.req.beforeStore) == 0 {
			if stats {
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
		if callbackFailedReq != nil {
			return callbackFailedReq, false, nil
		}
		return nil, true, fatalErr
	}
	if err = advanceBucketSequence(bucket); err != nil {
		if stats {
			db.autoBatcher.txOpErrors.Add(1)
		}
		rollbackCreated(accepted)
		db.mu.Unlock()
		return nil, true, err
	}

	publishDelta := &preparedDelta
	publishDeltaOwned := false
	var acceptedDelta preparedSnapshotDelta
	if len(accepted) != len(prepared) {
		acceptedDelta = db.prepareSnapshotWriteDeltaPreparedNoLock(accepted)
		publishDelta = &acceptedDelta
		publishDeltaOwned = true
	}
	defer func() {
		if publishDeltaOwned {
			acceptedDelta.release()
		}
	}()

	seq := bucket.Sequence()
	snap := db.buildPreparedSnapshotWithAccumDeltaNoLock(seq, publishDelta)
	db.stageSnapshot(snap)
	if err = db.commit(tx, "batch"); err != nil {
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
	committed = true
	err = db.publishAfterCommitLocked(seq, "batch", func() {
		db.finishSnapshotPublishNoLock(snap, publishDelta.indexDelta, publishDelta.nilDelta, publishDelta.lenDelta, publishDelta.universeAdd, publishDelta.universeRem)
		publishDelta.releaseTransientUniverse()
	})
	if err == nil {
		if len(accepted) == len(prepared) {
			preparedDeltaOwned = false
		} else {
			publishDeltaOwned = false
		}
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
}
