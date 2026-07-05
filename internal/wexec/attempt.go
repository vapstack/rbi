package wexec

import (
	"bytes"
	"fmt"
	"math/bits"
	"slices"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

type prepared struct {
	req *request

	payload  []byte
	physical []byte
	idx      uint64
	idxNew   bool
	oldVal   unsafe.Pointer
	snapOld  unsafe.Pointer
	newVal   unsafe.Pointer
}

type AppliedBatch struct {
	Seq      uint64
	Snapshot *snapshot.View
	cleanup  *appliedBatchCleanup
}

var snapshotOldValueExists byte

type appliedBatchCleanup struct {
	att     *attemptState
	ex      *Executor
	reqs    []request
	batches []Batch
}

func (applied *AppliedBatch) Release() {
	cleanup := applied.cleanup
	if cleanup == nil {
		return
	}
	applied.cleanup = nil
	cleanup.release()
}

func (cleanup *appliedBatchCleanup) release() {
	if cleanup.att != nil {
		attemptStatePool.Put(cleanup.att)
		cleanup.att = nil
	}
	if cleanup.ex != nil {
		for i := range cleanup.reqs {
			cleanup.ex.releaseRequest(&cleanup.reqs[i])
		}
		requestScratchPool.Put(cleanup.reqs)
		cleanup.ex = nil
		cleanup.reqs = nil
	}
	if cleanup.batches != nil {
		for i := range cleanup.batches {
			cleanup.batches[i].Cancel()
		}
		batchSlicePool.Put(cleanup.batches)
		cleanup.batches = nil
	}
	appliedBatchCleanupPool.Put(cleanup)
}

func assignPreparedErr(ops []prepared, err error) {
	for _, op := range ops {
		if op.req.Err == nil {
			op.req.Err = err
		}
	}
}

type recordState struct {
	key    []byte
	keyBuf [8]byte

	idx      uint64
	idxKnown bool
	idxNew   bool
	exists   bool

	value           unsafe.Pointer
	ownedPayload    *bytes.Buffer
	payloadOff      uint8
	payloadKnown    bool
	borrowedPayload []byte
}

type attemptState struct {
	dataBucket   *bbolt.Bucket
	strmapBucket *bbolt.Bucket
	base         *attemptState
	statsEnabled bool
	release      func(unsafe.Pointer)
	singleState  bool

	prepared []prepared
	accepted []prepared

	preparedSnapshots []snapshot.BatchEntry
	acceptedSnapshots []snapshot.BatchEntry
	acceptedKeyDeltas []snapshot.KeyDelta

	stateByUintID      map[uint64]int
	stateByUintIDCap   int
	stateByStringID    map[string]int
	stateByStringIDCap int
	states             []recordState
	state              recordState

	ownedPayloads []*bytes.Buffer
	releaseValues []unsafe.Pointer
	uniqueIdxs    []uint64
	uniqueOldVals []unsafe.Pointer
	uniqueNewVals []unsafe.Pointer
	uniqueState   uniqueBatchCheckState
}

func (b *Executor) prepareActive(att *attemptState, active []request, hookTx unsafe.Pointer) {
	for i := 0; i < len(active); i++ {
		req := &active[i]
		if req.Err != nil {
			continue
		}
		b.prepareRequest(att, req, hookTx)
	}
}

func (b *Executor) prepareRequest(att *attemptState, req *request, hookTx unsafe.Pointer) {
	switch req.op {
	case opSet:
		b.prepareSet(att, req, hookTx)
	case opPatch:
		b.preparePatch(att, req, hookTx)
	case opDelete:
		b.prepareDelete(att, req, hookTx)
	}
}

func (b *Executor) ensureIdx(att *attemptState, state *recordState, id keycodec.DataKey, create bool) error {
	if state.idxKnown {
		return nil
	}
	if b.strKey {
		idx, err := att.strmapBucket.NextSequence()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrAdvanceStringMapSequence, err)
		}
		state.idx = idx
		if create {
			state.idxNew = true
		}
	} else {
		state.idx = id.Uint()
	}
	state.idxKnown = true
	return nil
}

func (b *Executor) prepareSet(att *attemptState, req *request, hookTx unsafe.Pointer) {
	needOldValue := (b.indexed && b.schema.HasQueryFields()) || len(req.onChange) > 0
	readState := b.strKey || needOldValue
	state, pos, err := b.loadState(att, req, readState, needOldValue)
	if err != nil {
		req.Err = formatPrepareErr(prepareErrDecode, err)
		return
	}

	oldVal := state.value
	snapOld := oldVal

	// key-only indexed snapshots exist only for string keys
	//
	// numeric key collection without indexed fields runs in transparent mode,
	// so wexec is not configured as indexed there
	if snapOld == nil && state.exists && b.strKey && b.snapshotOps.StrKeyIndex && !b.schema.HasQueryFields() {
		snapOld = unsafe.Pointer(&snapshotOldValueExists)
	}

	payload := req.payloadBytes()
	physical := payload
	if req.setPayload != nil {
		physical = req.setPayload.Bytes()
	}

	var (
		newVal       unsafe.Pointer
		ownedPayload *bytes.Buffer
	)
	payloadOff := req.payloadOff

	if len(state.key) > bbolt.MaxKeySize {
		req.Err = formatBoltWriteErr(berrors.ErrKeyTooLarge, req.op, req.id.Format(b.strKey), state.idx, state.key, payload)
		return
	}

	if len(req.onChange) > 0 {
		if att.statsEnabled {
			b.stats.CallbackOps.Add(1)
		}
		if req.cloneValue != nil {
			newVal, err = req.cloneValue(req.id, req.setBaseline)
			if err != nil {
				req.Err = err
				return
			}
			att.releaseValues = append(att.releaseValues, newVal)
		} else {
			newVal, err = b.ops.Decode(req.payloadBytes())
			if err != nil {
				req.Err = formatPrepareErr(prepareErrDecodePreparedValue, err)
				return
			}
			att.releaseValues = append(att.releaseValues, newVal)
		}
		if err = runOnChangeHooks(hookTx, req.generationDepth, req.id, oldVal, newVal, req.onChange); err != nil {
			req.Err = err
			if att.statsEnabled {
				b.stats.CallbackErrors.Add(1)
			}
			return
		}

		if err = b.ops.ValidateIndex(newVal); err != nil {
			req.Err = err
			return
		}

		buf := encodePool.Get()
		payloadOff = reserveStringValuePrefix(buf, b.strKey)
		if err = b.ops.Encode(newVal, buf); err != nil {
			encodePool.Put(buf)
			req.Err = formatPrepareErr(prepareErrEncode, err)
			return
		}
		ownedPayload = buf
		att.ownedPayloads = append(att.ownedPayloads, buf)
		physical = buf.Bytes()
		payload = physical[payloadOff:]
	}

	if len(req.onChange) == 0 {
		needPreparedValue := b.indexed && b.schema.HasQueryFields()
		if !needPreparedValue && b.unique.Schema != nil && len(b.unique.Schema.Unique) != 0 {
			needPreparedValue = true
		}
		if needPreparedValue {
			if b.rejectEmptyPayload && len(payload) == 0 {
				req.Err = formatPrepareErr(prepareErrDecodePreparedValue, errEmptyPayload)
				return
			}
			newVal, err = b.ops.Decode(payload)
			if err != nil {
				req.Err = formatPrepareErr(prepareErrDecodePreparedValue, err)
				return
			}
			att.releaseValues = append(att.releaseValues, newVal)
		}
		if newVal != nil {
			if err = b.ops.ValidateIndex(newVal); err != nil {
				req.Err = err
				return
			}
		}
	}

	if b.rejectEmptyPayload && len(payload) == 0 {
		req.Err = formatBoltWriteErr(errEmptyPayload, req.op, req.id.Format(b.strKey), state.idx, state.key, payload)
		return
	}
	if int64(len(physical)) > bbolt.MaxValueSize {
		req.Err = formatBoltWriteErr(berrors.ErrValueTooLarge, req.op, req.id.Format(b.strKey), state.idx, state.key, payload)
		return
	}
	if b.strKey && !state.exists {
		// A key deleted earlier in this attempt no longer has a string-map row;
		// force a fresh idx so applyAccepted recreates the reverse mapping.
		state.idxKnown = false
		state.idxNew = false
	}
	if b.strKey || b.indexed {
		if err = b.ensureIdx(att, state, req.id, true); err != nil {
			req.Err = err
			return
		}
	}
	idxNew := state.idxNew
	snapNew := newVal
	if snapNew == nil && b.strKey && b.snapshotOps.StrKeyIndex && !b.schema.HasQueryFields() {
		snapNew = unsafe.Pointer(&snapshotOldValueExists)
	}

	att.prepared = append(att.prepared, prepared{
		req:      req,
		payload:  payload,
		physical: physical,
		idx:      state.idx,
		idxNew:   idxNew,
		oldVal:   oldVal,
		snapOld:  snapOld,
		newVal:   newVal,
	})

	if b.indexed {
		att.preparedSnapshots = append(att.preparedSnapshots, snapshot.BatchEntry{
			ID:  state.idx,
			Old: snapOld,
			New: snapNew,
		})
	}

	state.value = newVal
	state.exists = true

	if ownedPayload != nil {
		state.setOwnedPayload(ownedPayload, payloadOff)
	} else {
		state.setBorrowedPayload(payload)
	}

	if pos < 0 && !att.singleState {
		pos = len(att.states)
		att.states = append(att.states, *state)
		state = &att.states[pos]
		state.key = req.id.Bytes(b.strKey, &state.keyBuf)
		att.setStatePos(b.strKey, req.id, pos)
	}
}

func (b *Executor) preparePatch(att *attemptState, req *request, hookTx unsafe.Pointer) {
	state, _, err := b.loadState(att, req, true, true)
	if err != nil {
		req.Err = formatPrepareErr(prepareErrDecodeExistingValue, err)
		return
	}
	if state.value == nil {
		return
	}
	if !state.payloadKnown {
		req.Err = formatPrepareErr(prepareErrRedecodeEmpty, nil)
		return
	}

	oldVal := state.value
	snapOld := oldVal
	newVal, err := b.ops.Decode(state.payloadBytes())
	if err != nil {
		req.Err = formatPrepareErr(prepareErrRedecodeValue, err)
		return
	}
	att.releaseValues = append(att.releaseValues, newVal)
	if err = b.schema.Patch.Apply(newVal, req.patch, req.patchIgnoreUnknown); err != nil {
		req.Err = formatPrepareErr(prepareErrApplyPatch, err)
		return
	}
	if len(req.onChange) > 0 {
		if att.statsEnabled {
			b.stats.CallbackOps.Add(1)
		}
		if err = runOnChangeHooks(hookTx, req.generationDepth, req.id, oldVal, newVal, req.onChange); err != nil {
			req.Err = err
			if att.statsEnabled {
				b.stats.CallbackErrors.Add(1)
			}
			return
		}
	}

	if err = b.ops.ValidateIndex(newVal); err != nil {
		req.Err = err
		return
	}

	buf := encodePool.Get()
	payloadOff := reserveStringValuePrefix(buf, b.strKey)
	if err = b.ops.Encode(newVal, buf); err != nil {
		encodePool.Put(buf)
		req.Err = formatPrepareErr(prepareErrEncode, err)
		return
	}
	att.ownedPayloads = append(att.ownedPayloads, buf)

	payload := buf.Bytes()[payloadOff:]
	physical := buf.Bytes()
	if len(state.key) > bbolt.MaxKeySize {
		req.Err = formatBoltWriteErr(berrors.ErrKeyTooLarge, req.op, req.id.Format(b.strKey), state.idx, state.key, payload)
		return
	}
	if b.rejectEmptyPayload && len(payload) == 0 {
		req.Err = formatBoltWriteErr(errEmptyPayload, req.op, req.id.Format(b.strKey), state.idx, state.key, payload)
		return
	}
	if int64(len(physical)) > bbolt.MaxValueSize {
		req.Err = formatBoltWriteErr(berrors.ErrValueTooLarge, req.op, req.id.Format(b.strKey), state.idx, state.key, payload)
		return
	}
	if b.strKey || b.indexed {
		if err = b.ensureIdx(att, state, req.id, false); err != nil {
			req.Err = err
			return
		}
	}

	att.prepared = append(att.prepared, prepared{
		req:      req,
		payload:  payload,
		physical: physical,
		idx:      state.idx,
		idxNew:   state.idxNew,
		oldVal:   oldVal,
		snapOld:  snapOld,
		newVal:   newVal,
	})
	if b.indexed {
		att.preparedSnapshots = append(att.preparedSnapshots, snapshot.BatchEntry{
			ID:        state.idx,
			Old:       snapOld,
			New:       newVal,
			Patch:     req.patch,
			PatchOnly: len(req.onChange) == 0,
		})
	}
	state.value = newVal
	state.exists = true
	state.setOwnedPayload(buf, payloadOff)
}

func (b *Executor) prepareDelete(att *attemptState, req *request, hookTx unsafe.Pointer) {
	needOldValue := (b.indexed && b.schema.HasQueryFields()) || len(req.onChange) > 0
	state, _, err := b.loadState(att, req, true, needOldValue)
	if err != nil {
		req.Err = formatPrepareErr(prepareErrDecode, err)
		return
	}
	if !state.exists {
		return
	}

	oldVal := state.value
	snapOld := oldVal
	if snapOld == nil && !needOldValue {
		snapOld = unsafe.Pointer(&snapshotOldValueExists)
	}
	if b.strKey || b.indexed {
		if err = b.ensureIdx(att, state, req.id, false); err != nil {
			req.Err = err
			return
		}
	}
	if len(req.onChange) > 0 {
		if att.statsEnabled {
			b.stats.CallbackOps.Add(1)
		}
		if err = runOnChangeHooks(hookTx, req.generationDepth, req.id, oldVal, nil, req.onChange); err != nil {
			req.Err = err
			if att.statsEnabled {
				b.stats.CallbackErrors.Add(1)
			}
			return
		}
	}

	att.prepared = append(att.prepared, prepared{
		req:     req,
		idx:     state.idx,
		idxNew:  state.idxNew,
		oldVal:  oldVal,
		snapOld: snapOld,
		newVal:  nil,
	})
	if b.indexed {
		att.preparedSnapshots = append(att.preparedSnapshots, snapshot.BatchEntry{
			ID:  state.idx,
			Old: snapOld,
		})
	}
	state.value = nil
	state.exists = false
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

func (b *Executor) loadState(st *attemptState, req *request, read bool, decodeOld bool) (*recordState, int, error) {
	if st.singleState {
		state := &st.state
		state.key = req.id.Bytes(b.strKey, &state.keyBuf)
		if read {
			if prev := st.dataBucket.Get(state.key); prev != nil {
				state.exists = true
				payload := prev
				if b.strKey {
					if len(prev) < stringValuePrefixLen {
						return nil, -1, fmt.Errorf("string storage format: value shorter than %d bytes", stringValuePrefixLen)
					}
					idx := keycodec.U64FromBytes(prev[:stringValuePrefixLen])
					if idx == 0 {
						return nil, -1, fmt.Errorf("string storage format: zero string id")
					}
					state.idx = idx
					state.idxKnown = true
					payload = prev[stringValuePrefixLen:]
				}
				if decodeOld {
					oldVal, err := b.ops.Decode(payload)
					if err != nil {
						return nil, -1, err
					}
					state.value = oldVal
					st.releaseValues = append(st.releaseValues, oldVal)
				}
				state.setBorrowedPayload(payload)
			}
		}

		if decodeOld && state.exists && state.value == nil && state.payloadKnown {
			oldVal, err := b.ops.Decode(state.payloadBytes())
			if err != nil {
				return nil, -1, err
			}
			state.value = oldVal
			st.releaseValues = append(st.releaseValues, oldVal)
		}
		return state, -1, nil
	}

	if pos, ok := st.statePos(b.strKey, req.id); ok {
		state := &st.states[pos]
		if decodeOld && state.exists && state.value == nil && state.payloadKnown {
			oldVal, err := b.ops.Decode(state.payloadBytes())
			if err != nil {
				return nil, -1, err
			}
			state.value = oldVal
			st.releaseValues = append(st.releaseValues, oldVal)
		}
		return state, pos, nil
	}

	if st.base != nil {
		if basePos, ok := st.base.statePos(b.strKey, req.id); ok {
			// Frame current state copies accepted state lazily; failed current
			// work can then be discarded without rewriting the base attempt.
			pos := len(st.states)
			st.states = append(st.states, recordState{})
			state := &st.states[pos]
			*state = st.base.states[basePos]
			state.key = req.id.Bytes(b.strKey, &state.keyBuf)
			if decodeOld && state.exists && state.value == nil && state.payloadKnown {
				oldVal, err := b.ops.Decode(state.payloadBytes())
				if err != nil {
					*state = recordState{}
					st.states = st.states[:pos]
					return nil, -1, err
				}
				state.value = oldVal
				st.releaseValues = append(st.releaseValues, oldVal)
			}
			st.setStatePos(b.strKey, req.id, pos)
			return state, pos, nil
		}
	}
	if !read {
		state := &st.state
		*state = recordState{}
		state.key = req.id.Bytes(b.strKey, &state.keyBuf)
		return state, -1, nil
	}

	pos := len(st.states)
	st.states = append(st.states, recordState{})
	state := &st.states[pos]
	state.key = req.id.Bytes(b.strKey, &state.keyBuf)
	if prev := st.dataBucket.Get(state.key); prev != nil {
		state.exists = true
		payload := prev
		if b.strKey {
			if len(prev) < stringValuePrefixLen {
				*state = recordState{}
				st.states = st.states[:pos]
				return nil, -1, fmt.Errorf("string storage format: value shorter than %d bytes", stringValuePrefixLen)
			}
			idx := keycodec.U64FromBytes(prev[:stringValuePrefixLen])
			if idx == 0 {
				*state = recordState{}
				st.states = st.states[:pos]
				return nil, -1, fmt.Errorf("string storage format: zero string id")
			}
			state.idx = idx
			state.idxKnown = true
			payload = prev[stringValuePrefixLen:]
		}
		if decodeOld {
			oldVal, err := b.ops.Decode(payload)
			if err != nil {
				*state = recordState{}
				st.states = st.states[:pos]
				return nil, -1, err
			}
			state.value = oldVal
			st.releaseValues = append(st.releaseValues, oldVal)
		}
		state.setBorrowedPayload(payload)
	}

	st.setStatePos(b.strKey, req.id, pos)
	return state, pos, nil
}

func (b *Executor) applyAccepted(att *attemptState) error {
	stats := b.stats.Enabled

	for _, op := range att.accepted {
		var (
			err    error
			keyBuf [8]byte
		)
		key := op.req.id.Bytes(b.strKey, &keyBuf)

		switch op.req.op {

		case opSet, opPatch:
			if b.strKey {
				if op.idxNew {
					var mapKey [8]byte
					err = att.strmapBucket.Put(keycodec.U64BytesWithBuf(op.idx, &mapKey), key)
				}
				if err == nil {
					var idxBuf [8]byte
					// The encoded value reserved this prefix; fill it only after
					// filtering accepts the final string id.
					copy(op.physical, keycodec.U64BytesWithBuf(op.idx, &idxBuf))
					err = att.dataBucket.Put(key, op.physical)
				}
			} else {
				err = att.dataBucket.Put(key, op.physical)
			}

		case opDelete:
			err = att.dataBucket.Delete(key)
			if err == nil && b.strKey {
				var mapKey [8]byte
				err = att.strmapBucket.Delete(keycodec.U64BytesWithBuf(op.idx, &mapKey))
			}
		}

		if err != nil {
			err = formatBoltWriteErr(err, op.req.op, op.req.id.Format(b.strKey), op.idx, key, op.payload)
			if stats {
				b.stats.TxOpErrors.Add(1)
			}
			return err
		}
	}

	return nil
}

func (batch *Batch) ApplyAtomic(tx *bbolt.Tx) (AppliedBatch, error) {
	reqs := batch.reqs
	batch.reqs = nil
	batch.hooks = false

	if len(reqs) == 0 {
		requestScratchPool.Put(reqs)
		return AppliedBatch{}, nil
	}

	applied, err := batch.ex.applyAtomic(tx, reqs)
	if err != nil || applied.Seq == 0 {
		for i := 0; i < len(reqs); i++ {
			batch.ex.releaseRequest(&reqs[i])
		}
		requestScratchPool.Put(reqs)
		return applied, err
	}
	applied.cleanup.ex = batch.ex
	applied.cleanup.reqs = reqs
	return applied, err
}

func (b *Executor) applyAtomic(tx *bbolt.Tx, active []request) (AppliedBatch, error) {
	bucket := tx.Bucket(b.dataBucket)
	if bucket == nil {
		if b.stats.Enabled {
			b.stats.TxOpErrors.Add(1)
		}
		return AppliedBatch{}, ErrBucketMissing
	}

	bucket.FillPercent = b.bucketFillPercent

	var stringMap *bbolt.Bucket
	if b.strKey {
		stringMap = tx.Bucket(b.strmapBucket)
		if stringMap == nil {
			if b.stats.Enabled {
				b.stats.TxOpErrors.Add(1)
			}
			return AppliedBatch{}, ErrStringMapBucketMissing
		}
	}

	att, withSnapshots := b.newAttemptState(bucket, stringMap, len(active), len(active) == 1)
	var hookTx unsafe.Pointer

	b.prepareActive(att, active, hookTx)

	if reqErr := firstRequestErr(active); reqErr != nil {
		attemptStatePool.Put(att)
		return AppliedBatch{}, reqErr
	}
	if len(att.prepared) == 0 {
		attemptStatePool.Put(att)
		return AppliedBatch{}, nil
	}
	if err := b.filterAccepted(att); err != nil {
		attemptStatePool.Put(att)
		return AppliedBatch{}, err
	}
	if len(att.accepted) == 0 {
		attemptStatePool.Put(att)
		return AppliedBatch{}, nil
	}
	if err := b.applyAccepted(att); err != nil {
		attemptStatePool.Put(att)
		return AppliedBatch{}, err
	}

	seq, err := bucket.NextSequence()
	if err != nil {
		if b.stats.Enabled {
			b.stats.TxOpErrors.Add(1)
		}
		attemptStatePool.Put(att)
		return AppliedBatch{}, fmt.Errorf("%w: %w", ErrAdvanceBucketSequence, err)
	}

	cleanup := appliedBatchCleanupPool.Get()
	cleanup.att = att
	applied := AppliedBatch{Seq: seq, cleanup: cleanup}
	if !withSnapshots {
		return applied, nil
	}

	applied.Snapshot = b.buildAcceptedSnapshot(seq, att)
	return applied, nil
}

func (b *Executor) newAttemptState(bucket *bbolt.Bucket, stringMap *bbolt.Bucket, capHint int, singleState bool) (*attemptState, bool) {
	capHint = normalizeAttemptCapHint(capHint)
	uniqueFields := 0
	if b.unique.Schema != nil {
		uniqueFields = len(b.unique.Schema.Unique)
	}
	withSnapshots := b.snapshotsEnabled()
	att := attemptStatePool.Get()
	att.prepare(bucket, b.stats.Enabled, b.ops.Release, capHint, singleState, withSnapshots, uniqueFields)
	att.strmapBucket = stringMap
	if cap(att.prepared) < capHint {
		att.prepared = slices.Grow(att.prepared, capHint)
	}
	if !singleState {
		if cap(att.states) < capHint {
			att.states = slices.Grow(att.states, capHint)
		}
		att.prepareStateMap(b.strKey, capHint)
	}
	return att, withSnapshots
}

func normalizeAttemptCapHint(capHint int) int {
	if capHint == 0 {
		return 1
	}
	if capHint <= 8 {
		return 8
	}
	return 1 << bits.Len(uint(capHint-1))
}

func (b *Executor) snapshotsEnabled() bool {
	return b.indexed && b.snapshotOps.Current != nil
}

func (b *Executor) buildAcceptedSnapshot(seq uint64, att *attemptState) *snapshot.View {
	keyDeltas := b.acceptedKeyDeltas(att)
	return snapshot.BuildInPlaceWithKeyDeltas(
		seq,
		b.snapshotOps.Current(),
		b.snapshotOps.Schema,
		b.snapshotOps.CacheConfig,
		b.snapshotOps.PatchFields,
		att.acceptedSnapshots,
		keyDeltas,
	)
}

func (b *Executor) acceptedKeyDeltas(att *attemptState) []snapshot.KeyDelta {
	if !b.strKey || !b.snapshotOps.StrKeyIndex {
		return nil
	}
	if cap(att.acceptedKeyDeltas) < len(att.accepted) {
		att.acceptedKeyDeltas = slices.Grow(att.acceptedKeyDeltas, len(att.accepted))
	}
	keyDeltas := att.acceptedKeyDeltas[:0]
	for i := range att.accepted {
		op := att.accepted[i]
		switch op.req.op {
		case opSet:
			if op.idxNew {
				keyDeltas = append(keyDeltas, snapshot.KeyDelta{ID: op.idx, Key: op.req.id.String(), Add: true})
			}
		case opDelete:
			if op.snapOld != nil {
				keyDeltas = append(keyDeltas, snapshot.KeyDelta{ID: op.idx, Key: op.req.id.String()})
			}
		}
	}
	att.acceptedKeyDeltas = keyDeltas
	return keyDeltas
}

func (st *attemptState) prepare(bucket *bbolt.Bucket, statsEnabled bool, release func(unsafe.Pointer), capHint int, singleState bool, withSnapshots bool, uniqueFields int) {
	st.dataBucket = bucket
	st.statsEnabled = statsEnabled
	st.release = release
	st.singleState = singleState
	if cap(st.ownedPayloads) < capHint {
		st.ownedPayloads = slices.Grow(st.ownedPayloads, capHint)
	}
	releaseCapHint := capHint * 2
	if cap(st.releaseValues) < releaseCapHint {
		st.releaseValues = slices.Grow(st.releaseValues, releaseCapHint)
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
	for _, ptr := range st.releaseValues {
		st.release(ptr)
	}
	clear(st.releaseValues)
	st.releaseValues = st.releaseValues[:0]

	for _, buf := range st.ownedPayloads {
		encodePool.Put(buf)
	}
	clear(st.ownedPayloads)
	st.ownedPayloads = st.ownedPayloads[:0]

	clear(st.preparedSnapshots)
	st.preparedSnapshots = st.preparedSnapshots[:0]

	clear(st.acceptedSnapshots)
	st.acceptedSnapshots = st.acceptedSnapshots[:0]

	clear(st.acceptedKeyDeltas)
	st.acceptedKeyDeltas = st.acceptedKeyDeltas[:0]

	st.uniqueIdxs = st.uniqueIdxs[:0]

	clear(st.uniqueOldVals)
	st.uniqueOldVals = st.uniqueOldVals[:0]

	clear(st.uniqueNewVals)
	st.uniqueNewVals = st.uniqueNewVals[:0]

	st.uniqueState.cleanup()

	st.dataBucket = nil
	st.strmapBucket = nil
	st.base = nil
	st.statsEnabled = false
	st.release = nil
	st.singleState = false
	st.state = recordState{}
}

func (st *recordState) payloadBytes() []byte {
	if st.ownedPayload != nil {
		return st.ownedPayload.Bytes()[st.payloadOff:]
	}
	return st.borrowedPayload
}

func (st *recordState) setOwnedPayload(buf *bytes.Buffer, payloadOff uint8) {
	st.ownedPayload = buf
	st.payloadOff = payloadOff
	st.payloadKnown = true
	st.borrowedPayload = nil
}

func (st *recordState) setBorrowedPayload(payload []byte) {
	st.ownedPayload = nil
	st.payloadOff = 0
	st.payloadKnown = true
	st.borrowedPayload = payload
}

func (st *recordState) clearPayload() {
	st.ownedPayload = nil
	st.payloadOff = 0
	st.payloadKnown = false
	st.borrowedPayload = nil
}

func reserveStringValuePrefix(buf *bytes.Buffer, strKey bool) uint8 {
	if !strKey {
		return 0
	}
	var prefix [stringValuePrefixLen]byte
	buf.Write(prefix[:])
	return stringValuePrefixLen
}
