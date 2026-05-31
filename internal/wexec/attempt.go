package wexec

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
)

type prepared struct {
	req *request

	key     []byte
	payload []byte
	idx     uint64
	idxNew  bool
	oldVal  unsafe.Pointer
	newVal  unsafe.Pointer
}

func assignPreparedErr(ops []prepared, err error) {
	for _, op := range ops {
		if op.req.Err == nil {
			op.req.Err = err
		}
	}
}

func (b *Batcher) rollbackCreatedPrepared(ops []prepared) {
	if !b.strKey {
		return
	}
	for i := range ops {
		op := ops[i]
		if op.idxNew && op.oldVal == nil && op.newVal != nil {
			b.strMap.RollbackCreated(op.req.id.String(), op.idx)
		}
	}
}

type recordState struct {
	key    []byte
	keyBuf [8]byte

	idx      uint64
	idxKnown bool
	idxNew   bool

	value           unsafe.Pointer
	ownedPayload    *bytes.Buffer
	borrowedPayload []byte
}

type attemptState struct {
	bucket       *bbolt.Bucket
	statsEnabled bool
	release      func(unsafe.Pointer)
	singleState  bool

	prepared []prepared
	accepted []prepared

	preparedSnapshots []snapshot.BatchEntry
	acceptedSnapshots []snapshot.BatchEntry

	stateByUintID      map[uint64]int
	stateByUintIDCap   int
	stateByStringID    map[string]int
	stateByStringIDCap int
	states             []recordState
	state              recordState

	ownedPayloads []*bytes.Buffer
	decodedValues []unsafe.Pointer

	uniqueIdxs    []uint64
	uniqueOldVals []unsafe.Pointer
	uniqueNewVals []unsafe.Pointer
	uniqueState   uniqueBatchCheckState
}

func (b *Batcher) prepareActive(att *attemptState, active []*request) {
	for i := 0; i < len(active); i++ {
		req := active[i]
		if req.Err != nil {
			continue
		}

		switch req.op {
		case opSet:
			b.prepareSet(att, req)
		case opPatch:
			b.preparePatch(att, req)
		case opDelete:
			b.prepareDelete(att, req)
		}
	}
}

func (b *Batcher) ensureIdx(state *recordState, id keycodec.DataKey, create bool) {
	if state.idxKnown {
		return
	}
	if b.strKey {
		var created bool
		state.idx, created = b.strMap.Create(id.String())
		if create {
			state.idxNew = created
		}
	} else {
		state.idx = id.Uint()
	}
	state.idxKnown = true
}

func (b *Batcher) prepareSet(att *attemptState, req *request) {
	state, err := att.loadState(b.strKey, b.ops, req, b.indexed || len(req.beforeStore) > 0 || len(req.beforeCommit) > 0)
	if err != nil {
		req.Err = formatPrepareErr(prepareErrDecode, err)
		return
	}

	oldVal := state.value
	newVal := req.setValue
	payload := req.payloadBytes()
	var ownedPayload *bytes.Buffer

	if len(req.beforeStore) > 0 {
		if att.statsEnabled {
			b.sched.stats.CallbackOps.Add(1)
		}
		if req.cloneValue != nil {
			newVal, err = req.cloneValue(req.id, req.setBaseline)
			if err != nil {
				req.Err = err
				return
			}
		} else {
			newVal, err = b.ops.Decode(req.payloadBytes())
			if err != nil {
				req.Err = formatPrepareErr(prepareErrDecodePreparedValue, err)
				return
			}
			att.decodedValues = append(att.decodedValues, newVal)
		}
		if err = runBeforeStoreHooks(req.id, oldVal, newVal, req.beforeStore); err != nil {
			req.Err = err
			if att.statsEnabled {
				b.sched.stats.CallbackErrors.Add(1)
			}
			return
		}

		if err = b.ops.ValidateIndex(newVal); err != nil {
			req.Err = err
			return
		}

		buf := encodePool.Get()
		if err = b.ops.Encode(newVal, buf); err != nil {
			encodePool.Put(buf)
			req.Err = formatPrepareErr(prepareErrEncode, err)
			return
		}
		ownedPayload = buf
		att.ownedPayloads = append(att.ownedPayloads, buf)
		payload = buf.Bytes()
	}

	if len(req.beforeStore) == 0 {
		if err = b.ops.ValidateIndex(newVal); err != nil {
			req.Err = err
			return
		}
	}

	if b.indexed {
		b.ensureIdx(state, req.id, true)
	}

	att.prepared = append(att.prepared, prepared{
		req:     req,
		key:     state.key,
		payload: payload,
		idx:     state.idx,
		idxNew:  state.idxNew,
		oldVal:  oldVal,
		newVal:  newVal,
	})
	if b.indexed {
		att.preparedSnapshots = append(att.preparedSnapshots, snapshot.BatchEntry{
			ID:  state.idx,
			Old: oldVal,
			New: newVal,
		})
	}
	state.value = newVal
	if ownedPayload != nil {
		state.setOwnedPayload(ownedPayload)
	} else {
		state.setBorrowedPayload(payload)
	}
}

func (b *Batcher) preparePatch(att *attemptState, req *request) {
	state, err := att.loadState(b.strKey, b.ops, req, true)
	if err != nil {
		req.Err = formatPrepareErr(prepareErrDecodeExistingValue, err)
		return
	}
	if state.value == nil {
		return
	}
	if state.ownedPayload == nil && state.borrowedPayload == nil {
		req.Err = formatPrepareErr(prepareErrRedecodeEmpty, nil)
		return
	}

	oldVal := state.value
	newVal, err := b.ops.Decode(state.payloadBytes())
	if err != nil {
		req.Err = formatPrepareErr(prepareErrRedecodeValue, err)
		return
	}
	att.decodedValues = append(att.decodedValues, newVal)
	if err = b.schema.Patch.Apply(newVal, req.patch, req.patchIgnoreUnknown); err != nil {
		req.Err = formatPrepareErr(prepareErrApplyPatch, err)
		return
	}
	if len(req.beforeProcess) > 0 {
		if err = runBeforeProcessHooks(req.id, newVal, req.beforeProcess); err != nil {
			req.Err = err
			return
		}
	}
	if len(req.beforeStore) > 0 {
		if att.statsEnabled {
			b.sched.stats.CallbackOps.Add(1)
		}
		if err = runBeforeStoreHooks(req.id, oldVal, newVal, req.beforeStore); err != nil {
			req.Err = err
			if att.statsEnabled {
				b.sched.stats.CallbackErrors.Add(1)
			}
			return
		}
	}

	if err = b.ops.ValidateIndex(newVal); err != nil {
		req.Err = err
		return
	}

	buf := encodePool.Get()
	if err = b.ops.Encode(newVal, buf); err != nil {
		encodePool.Put(buf)
		req.Err = formatPrepareErr(prepareErrEncode, err)
		return
	}
	att.ownedPayloads = append(att.ownedPayloads, buf)

	if b.indexed {
		b.ensureIdx(state, req.id, false)
	}

	att.prepared = append(att.prepared, prepared{
		req:     req,
		key:     state.key,
		payload: buf.Bytes(),
		idx:     state.idx,
		idxNew:  state.idxNew,
		oldVal:  oldVal,
		newVal:  newVal,
	})
	if b.indexed {
		att.preparedSnapshots = append(att.preparedSnapshots, snapshot.BatchEntry{
			ID:        state.idx,
			Old:       oldVal,
			New:       newVal,
			Patch:     req.patch,
			PatchOnly: len(req.beforeProcess) == 0 && len(req.beforeStore) == 0,
		})
	}
	state.value = newVal
	state.setOwnedPayload(buf)
}

func (b *Batcher) prepareDelete(att *attemptState, req *request) {
	state, err := att.loadState(b.strKey, b.ops, req, true)
	if err != nil {
		req.Err = formatPrepareErr(prepareErrDecode, err)
		return
	}
	if state.value == nil {
		return
	}

	oldVal := state.value
	if b.indexed {
		b.ensureIdx(state, req.id, false)
	}

	att.prepared = append(att.prepared, prepared{
		req:    req,
		key:    state.key,
		idx:    state.idx,
		idxNew: state.idxNew,
		oldVal: oldVal,
		newVal: nil,
	})
	if b.indexed {
		att.preparedSnapshots = append(att.preparedSnapshots, snapshot.BatchEntry{
			ID:  state.idx,
			Old: oldVal,
		})
	}
	state.value = nil
	state.clearPayload()
}

func (st *attemptState) prepareStateMap(strKey bool, capHint int) {
	if strKey {
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

func (st *attemptState) statePos(strKey bool, key keycodec.DataKey) (int, bool) {
	if strKey {
		pos, ok := st.stateByStringID[key.String()]
		return pos, ok
	}
	pos, ok := st.stateByUintID[key.Uint()]
	return pos, ok
}

func (st *attemptState) setStatePos(strKey bool, key keycodec.DataKey, pos int) {
	if strKey {
		st.stateByStringID[key.String()] = pos
		return
	}
	st.stateByUintID[key.Uint()] = pos
}

func (st *attemptState) loadState(strKey bool, ops *RecordOps, req *request, read bool) (*recordState, error) {
	if st.singleState {
		state := &st.state
		state.key = req.id.Bytes(strKey, &state.keyBuf)
		if read {
			if prev := st.bucket.Get(state.key); prev != nil {
				oldVal, err := ops.Decode(prev)
				if err != nil {
					return nil, err
				}
				state.value = oldVal
				st.decodedValues = append(st.decodedValues, oldVal)
				state.setBorrowedPayload(prev)
			}
		}
		return state, nil
	}

	if pos, ok := st.statePos(strKey, req.id); ok {
		return &st.states[pos], nil
	}

	pos := len(st.states)
	st.states = append(st.states, recordState{})
	state := &st.states[pos]
	state.key = req.id.Bytes(strKey, &state.keyBuf)
	if read {
		if prev := st.bucket.Get(state.key); prev != nil {
			oldVal, err := ops.Decode(prev)
			if err != nil {
				st.states = st.states[:pos]
				return nil, err
			}
			state.value = oldVal
			st.decodedValues = append(st.decodedValues, oldVal)
			state.setBorrowedPayload(prev)
		}
	}

	st.setStatePos(strKey, req.id, pos)
	return state, nil
}

func (b *Batcher) applyAccepted(tx *bbolt.Tx, bucket *bbolt.Bucket, accepted []prepared, atomicAll bool, rejectEmptyPayload bool) (*request, error) {
	stats := b.sched.stats.Enabled
	var callbackFailedReq *request

	for _, op := range accepted {
		var err error
		switch op.req.op {
		case opSet, opPatch:
			if rejectEmptyPayload && len(op.payload) == 0 {
				err = errEmptyPayload
			} else {
				err = bucket.Put(op.key, op.payload)
			}
		case opDelete:
			err = bucket.Delete(op.key)
		default:
			err = errUnknownOp
		}
		if err != nil {
			rawErr := err
			err = formatBoltWriteErr(err, op.req.op, op.req.id.Format(b.strKey), op.idx, op.key, op.payload)
			if errors.Is(rawErr, errEmptyPayload) {
				op.req.Err = err
			}
			if stats {
				b.sched.stats.TxOpErrors.Add(1)
			}
			if errors.Is(rawErr, errEmptyPayload) && !atomicAll {
				callbackFailedReq = op.req
				break
			}
			return nil, err
		}

		if len(op.req.beforeCommit) > 0 {
			if stats && len(op.req.beforeStore) == 0 {
				b.sched.stats.CallbackOps.Add(1)
			}
			if err = runBeforeCommitHooks(tx, op.req.id, op.oldVal, op.newVal, op.req.beforeCommit); err != nil {
				err = fmt.Errorf(
					"before_commit op=%s id=%s idx=%d key_len=%d payload_len=%d: %w",
					op.req.op.String(),
					op.req.id.Format(b.strKey),
					op.idx,
					len(op.key),
					len(op.payload),
					err,
				)
				op.req.Err = err
				if stats {
					b.sched.stats.CallbackErrors.Add(1)
					b.sched.stats.TxOpErrors.Add(1)
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

func (b *Batcher) attempt(active []*request, atomicAll bool) (*request, bool, error) {
	var firstOp Op
	if len(active) != 0 {
		firstOp = active[0].op
	}
	name := opName(firstOp, len(active), atomicAll, b.sched.maxOps)

	tx, err := b.bolt.Begin(true)
	if err != nil {
		if b.sched.stats.Enabled {
			b.sched.stats.TxBeginErrors.Add(1)
		}
		return nil, true, fmt.Errorf("tx error: %w", err)
	}
	bucket := tx.Bucket(b.bucket)
	if bucket == nil {
		if b.sched.stats.Enabled {
			b.sched.stats.TxOpErrors.Add(1)
		}
		_ = tx.Rollback()
		return nil, true, fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = b.bucketFillPercent
	defer func() { _ = tx.Rollback() }()

	capHint := len(active)
	singleState := capHint == 1
	if capHint == 0 {
		capHint = 1
	} else if capHint <= 8 {
		capHint = 8
	} else {
		capHint = 1 << bits.Len(uint(capHint-1))
	}
	att := attemptStatePool.Get()
	uniqueFields := 0
	if b.unique.Schema != nil {
		uniqueFields = len(b.unique.Schema.Unique)
	}
	withSnapshots := b.indexed && b.snapshotOps.Manager != nil
	att.prepare(bucket, b.sched.stats.Enabled, b.ops.Release, capHint, singleState, withSnapshots, uniqueFields)
	if cap(att.prepared) < capHint {
		att.prepared = slices.Grow(att.prepared, capHint)
	}
	if !singleState && cap(att.states) < capHint {
		att.states = slices.Grow(att.states, capHint)
	}

	if !singleState {
		att.prepareStateMap(b.strKey, capHint)
	}
	defer attemptStatePool.Put(att)

	b.prepareActive(att, active)

	if atomicAll {
		if reqErr := firstRequestErr(active); reqErr != nil {
			b.rollbackCreatedPrepared(att.prepared)
			return nil, true, reqErr
		}
	}

	if len(att.prepared) == 0 {
		return nil, true, nil
	}

	b.publishMu.Lock()
	defer b.publishMu.Unlock()

	if err = b.unavailable(); err != nil {
		b.rollbackCreatedPrepared(att.prepared)
		assignPreparedErr(att.prepared, err)
		return nil, true, nil
	}

	if err = b.filterAccepted(att, atomicAll); err != nil {
		b.rollbackCreatedPrepared(att.prepared)
		return nil, true, err
	}

	if len(att.accepted) == 0 {
		return nil, true, nil
	}

	retryWithoutReq, err := b.applyAccepted(tx, bucket, att.accepted, atomicAll, b.rejectEmptyPayload)
	if err != nil {
		b.rollbackCreatedPrepared(att.accepted)
		return nil, true, err
	}
	if retryWithoutReq != nil {
		b.rollbackCreatedPrepared(att.accepted)
		return retryWithoutReq, false, nil
	}

	if !withSnapshots {
		if _, err = bucket.NextSequence(); err != nil {
			if b.sched.stats.Enabled {
				b.sched.stats.TxOpErrors.Add(1)
			}
			b.rollbackCreatedPrepared(att.accepted)
			return nil, true, fmt.Errorf("advance bucket sequence: %w", err)
		}
		if err = b.commit(tx, name); err != nil {
			if b.sched.stats.Enabled {
				b.sched.stats.TxCommitErrors.Add(1)
			}
			assignPreparedErr(att.accepted, err)
		}
		return nil, true, nil
	}

	seq, err := bucket.NextSequence()
	if err != nil {
		if b.sched.stats.Enabled {
			b.sched.stats.TxOpErrors.Add(1)
		}
		b.rollbackCreatedPrepared(att.accepted)
		return nil, true, fmt.Errorf("advance bucket sequence: %w", err)
	}

	snap := snapshot.BuildPrepared(
		seq,
		b.snapshotOps.Manager.Current(),
		b.snapshotOps.Schema,
		b.snapshotOps.CacheConfig(),
		b.snapshotOps.StrMap,
		b.snapshotOps.PatchFields,
		att.acceptedSnapshots,
	)
	b.snapshotOps.Manager.Stage(snap)

	if err = b.commit(tx, name); err != nil {
		if b.sched.stats.Enabled {
			b.sched.stats.TxCommitErrors.Add(1)
		}
		b.snapshotOps.Manager.DropStaged(seq)
		b.rollbackCreatedPrepared(att.accepted)
		assignPreparedErr(att.accepted, err)
		return nil, true, nil
	}

	err = b.indexPublishOps.PublishCommitted(seq, name, snap)
	if err != nil {
		assignPreparedErr(att.accepted, err)
	}

	return nil, true, nil
}

func (st *attemptState) prepare(bucket *bbolt.Bucket, statsEnabled bool, release func(unsafe.Pointer), capHint int, singleState bool, withSnapshots bool, uniqueFields int) {
	st.bucket = bucket
	st.statsEnabled = statsEnabled
	st.release = release
	st.singleState = singleState
	if cap(st.ownedPayloads) < capHint {
		st.ownedPayloads = slices.Grow(st.ownedPayloads, capHint)
	}
	decodedCapHint := capHint * 2
	if cap(st.decodedValues) < decodedCapHint {
		st.decodedValues = slices.Grow(st.decodedValues, decodedCapHint)
	}
	if uniqueFields != 0 {
		if cap(st.uniqueIdxs) < capHint {
			st.uniqueIdxs = slices.Grow(st.uniqueIdxs, capHint)
		}
		if cap(st.uniqueOldVals) < capHint {
			st.uniqueOldVals = slices.Grow(st.uniqueOldVals, capHint)
		}
		if cap(st.uniqueNewVals) < capHint {
			st.uniqueNewVals = slices.Grow(st.uniqueNewVals, capHint)
		}
		st.uniqueState.prepare(uniqueFields)
	}
	if withSnapshots {
		if cap(st.preparedSnapshots) < capHint {
			st.preparedSnapshots = slices.Grow(st.preparedSnapshots, capHint)
		}
		if cap(st.acceptedSnapshots) < capHint {
			st.acceptedSnapshots = slices.Grow(st.acceptedSnapshots, capHint)
		}
	}
}

func (st *attemptState) cleanup() {
	for _, ptr := range st.decodedValues {
		st.release(ptr)
	}
	clear(st.decodedValues)
	st.decodedValues = st.decodedValues[:0]

	for _, buf := range st.ownedPayloads {
		encodePool.Put(buf)
	}
	clear(st.ownedPayloads)
	st.ownedPayloads = st.ownedPayloads[:0]

	clear(st.preparedSnapshots)
	st.preparedSnapshots = st.preparedSnapshots[:0]

	clear(st.acceptedSnapshots)
	st.acceptedSnapshots = st.acceptedSnapshots[:0]

	st.uniqueIdxs = st.uniqueIdxs[:0]

	clear(st.uniqueOldVals)
	st.uniqueOldVals = st.uniqueOldVals[:0]

	clear(st.uniqueNewVals)
	st.uniqueNewVals = st.uniqueNewVals[:0]

	st.uniqueState.cleanup()

	st.bucket = nil
	st.statsEnabled = false
	st.release = nil
	st.singleState = false
	st.state = recordState{}
}

func (st *recordState) payloadBytes() []byte {
	if st.ownedPayload != nil {
		return st.ownedPayload.Bytes()
	}
	return st.borrowedPayload
}

func (st *recordState) setOwnedPayload(buf *bytes.Buffer) {
	st.ownedPayload = buf
	st.borrowedPayload = nil
}

func (st *recordState) setBorrowedPayload(payload []byte) {
	st.ownedPayload = nil
	st.borrowedPayload = payload
}

func (st *recordState) clearPayload() {
	st.ownedPayload = nil
	st.borrowedPayload = nil
}
