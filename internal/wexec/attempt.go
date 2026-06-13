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

	key      []byte
	payload  []byte
	physical []byte
	idx      uint64
	idxNew   bool
	oldVal   unsafe.Pointer
	snapOld  unsafe.Pointer
	newVal   unsafe.Pointer
}

var snapshotOldValueExists byte

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
	borrowedPayload []byte
}

type attemptState struct {
	dataBucket   *bbolt.Bucket
	strmapBucket *bbolt.Bucket
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

func (b *Batcher) ensureIdx(att *attemptState, state *recordState, id keycodec.DataKey, create bool) error {
	if state.idxKnown {
		return nil
	}
	if b.strKey {
		idx, err := att.strmapBucket.NextSequence()
		if err != nil {
			return fmt.Errorf("advance string map sequence: %w", err)
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

func (b *Batcher) prepareSet(att *attemptState, req *request) {
	needOldValue := (b.indexed && b.schema.HasQueryFields()) || len(req.beforeStore) > 0 || len(req.beforeCommit) > 0
	state, err := b.loadState(att, req, b.strKey || needOldValue, needOldValue)
	if err != nil {
		req.Err = formatPrepareErr(prepareErrDecode, err)
		return
	}

	oldVal := state.value
	snapOld := oldVal
	if snapOld == nil && state.exists && b.strKey && b.snapshotOps.StrKeyIndex && !b.schema.HasQueryFields() {
		snapOld = unsafe.Pointer(&snapshotOldValueExists)
	}
	newVal := req.setValue
	payload := req.payloadBytes()
	physical := payload
	if req.setPayload != nil {
		physical = req.setPayload.Bytes()
	}
	var ownedPayload *bytes.Buffer
	payloadOff := req.payloadOff

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
			att.releaseValues = append(att.releaseValues, newVal)
		} else {
			newVal, err = b.ops.Decode(req.payloadBytes())
			if err != nil {
				req.Err = formatPrepareErr(prepareErrDecodePreparedValue, err)
				return
			}
			att.releaseValues = append(att.releaseValues, newVal)
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

	if len(req.beforeStore) == 0 {
		if err = b.ops.ValidateIndex(newVal); err != nil {
			req.Err = err
			return
		}
	}

	if b.strKey && !state.exists {
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

	att.prepared = append(att.prepared, prepared{
		req:      req,
		key:      state.key,
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
			New: newVal,
		})
	}
	state.value = newVal
	state.exists = true
	if ownedPayload != nil {
		state.setOwnedPayload(ownedPayload, payloadOff)
	} else {
		state.setBorrowedPayload(payload)
	}
}

func (b *Batcher) preparePatch(att *attemptState, req *request) {
	state, err := b.loadState(att, req, true, true)
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
	payloadOff := reserveStringValuePrefix(buf, b.strKey)
	if err = b.ops.Encode(newVal, buf); err != nil {
		encodePool.Put(buf)
		req.Err = formatPrepareErr(prepareErrEncode, err)
		return
	}
	att.ownedPayloads = append(att.ownedPayloads, buf)

	if b.strKey || b.indexed {
		if err = b.ensureIdx(att, state, req.id, false); err != nil {
			req.Err = err
			return
		}
	}

	att.prepared = append(att.prepared, prepared{
		req:      req,
		key:      state.key,
		payload:  buf.Bytes()[payloadOff:],
		physical: buf.Bytes(),
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
			PatchOnly: len(req.beforeProcess) == 0 && len(req.beforeStore) == 0,
		})
	}
	state.value = newVal
	state.exists = true
	state.setOwnedPayload(buf, payloadOff)
}

func (b *Batcher) prepareDelete(att *attemptState, req *request) {
	needOldValue := true
	if b.strKey && b.snapshotOps.StrKeyIndex && !b.schema.HasQueryFields() && len(req.beforeCommit) == 0 {
		needOldValue = false
	}
	state, err := b.loadState(att, req, true, needOldValue)
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

	att.prepared = append(att.prepared, prepared{
		req:     req,
		key:     state.key,
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

func (b *Batcher) loadState(st *attemptState, req *request, read bool, decodeOld bool) (*recordState, error) {
	if st.singleState {
		state := &st.state
		state.key = req.id.Bytes(b.strKey, &state.keyBuf)
		if read {
			if prev := st.dataBucket.Get(state.key); prev != nil {
				state.exists = true
				payload := prev
				if b.strKey {
					if len(prev) < stringValuePrefixLen {
						return nil, fmt.Errorf("string storage format: value shorter than %d bytes", stringValuePrefixLen)
					}
					idx := keycodec.U64FromBytes(prev[:stringValuePrefixLen])
					if idx == 0 {
						return nil, fmt.Errorf("string storage format: zero string id")
					}
					state.idx = idx
					state.idxKnown = true
					payload = prev[stringValuePrefixLen:]
				}
				if decodeOld {
					oldVal, err := b.ops.Decode(payload)
					if err != nil {
						return nil, err
					}
					state.value = oldVal
					st.releaseValues = append(st.releaseValues, oldVal)
				}
				state.setBorrowedPayload(payload)
			}
		}
		return state, nil
	}

	if pos, ok := st.statePos(b.strKey, req.id); ok {
		return &st.states[pos], nil
	}

	pos := len(st.states)
	st.states = append(st.states, recordState{})
	state := &st.states[pos]
	state.key = req.id.Bytes(b.strKey, &state.keyBuf)
	if read {
		if prev := st.dataBucket.Get(state.key); prev != nil {
			state.exists = true
			payload := prev
			if b.strKey {
				if len(prev) < stringValuePrefixLen {
					*state = recordState{}
					st.states = st.states[:pos]
					return nil, fmt.Errorf("string storage format: value shorter than %d bytes", stringValuePrefixLen)
				}
				idx := keycodec.U64FromBytes(prev[:stringValuePrefixLen])
				if idx == 0 {
					*state = recordState{}
					st.states = st.states[:pos]
					return nil, fmt.Errorf("string storage format: zero string id")
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
					return nil, err
				}
				state.value = oldVal
				st.releaseValues = append(st.releaseValues, oldVal)
			}
			state.setBorrowedPayload(payload)
		}
	}

	st.setStatePos(b.strKey, req.id, pos)
	return state, nil
}

func (b *Batcher) applyAccepted(tx *bbolt.Tx, bucket *bbolt.Bucket, att *attemptState, accepted []prepared, atomicAll bool, rejectEmptyPayload bool) (*request, error) {
	stats := b.sched.stats.Enabled
	var callbackFailedReq *request

	for _, op := range accepted {
		var err error
		switch op.req.op {
		case opSet, opPatch:
			if rejectEmptyPayload && len(op.payload) == 0 {
				err = errEmptyPayload
			} else if b.strKey {
				if op.idxNew {
					var mapKey [8]byte
					err = att.strmapBucket.Put(keycodec.U64BytesWithBuf(op.idx, &mapKey), op.key)
				}
				if err == nil {
					var idxBuf [8]byte
					copy(op.physical, keycodec.U64BytesWithBuf(op.idx, &idxBuf))
					err = bucket.Put(op.key, op.physical)
				}
			} else {
				err = bucket.Put(op.key, op.physical)
			}
		case opDelete:
			err = bucket.Delete(op.key)
			if err == nil && b.strKey {
				var mapKey [8]byte
				err = att.strmapBucket.Delete(keycodec.U64BytesWithBuf(op.idx, &mapKey))
			}
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
	tx, err := b.bolt.Begin(true)
	if err != nil {
		if b.sched.stats.Enabled {
			b.sched.stats.TxBeginErrors.Add(1)
		}
		return nil, true, fmt.Errorf("tx error: %w", err)
	}
	bucket := tx.Bucket(b.dataBucket)
	if bucket == nil {
		if b.sched.stats.Enabled {
			b.sched.stats.TxOpErrors.Add(1)
		}
		_ = tx.Rollback()
		return nil, true, fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = b.bucketFillPercent
	var stringMap *bbolt.Bucket
	if b.strKey {
		stringMap = tx.Bucket(b.strmapBucket)
		if stringMap == nil {
			if b.sched.stats.Enabled {
				b.sched.stats.TxOpErrors.Add(1)
			}
			_ = tx.Rollback()
			return nil, true, fmt.Errorf("string map bucket does not exist")
		}
	}
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
	att.strmapBucket = stringMap
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
			return nil, true, reqErr
		}
	}

	if len(att.prepared) == 0 {
		return nil, true, nil
	}

	b.publishMu.Lock()
	defer b.publishMu.Unlock()

	if err = b.unavailable(); err != nil {
		assignPreparedErr(att.prepared, err)
		return nil, true, nil
	}

	if err = b.filterAccepted(att, atomicAll); err != nil {
		return nil, true, err
	}

	if len(att.accepted) == 0 {
		return nil, true, nil
	}

	retryWithoutReq, err := b.applyAccepted(tx, bucket, att, att.accepted, atomicAll, b.rejectEmptyPayload)
	if err != nil {
		return nil, true, err
	}
	if retryWithoutReq != nil {
		return retryWithoutReq, false, nil
	}

	if !withSnapshots {
		if _, err = bucket.NextSequence(); err != nil {
			if b.sched.stats.Enabled {
				b.sched.stats.TxOpErrors.Add(1)
			}
			return nil, true, fmt.Errorf("advance bucket sequence: %w", err)
		}
		if err = b.commit(tx); err != nil {
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
		return nil, true, fmt.Errorf("advance bucket sequence: %w", err)
	}

	var keyDeltas []snapshot.KeyDelta
	if b.strKey && b.snapshotOps.StrKeyIndex {
		if cap(att.acceptedKeyDeltas) < len(att.accepted) {
			att.acceptedKeyDeltas = slices.Grow(att.acceptedKeyDeltas, len(att.accepted))
		}
		keyDeltas = att.acceptedKeyDeltas[:0]
		for i := range att.accepted {
			op := att.accepted[i]
			switch op.req.op {
			case opSet:
				if op.idxNew && op.newVal != nil {
					keyDeltas = append(keyDeltas, snapshot.KeyDelta{ID: op.idx, Key: op.req.id.String(), Add: true})
				}
			case opDelete:
				if op.snapOld != nil {
					keyDeltas = append(keyDeltas, snapshot.KeyDelta{ID: op.idx, Key: op.req.id.String()})
				}
			}
		}
		att.acceptedKeyDeltas = keyDeltas
	}

	snap := snapshot.BuildInPlaceWithKeyDeltas(
		seq,
		b.snapshotOps.Manager.Current(),
		b.snapshotOps.Schema,
		b.snapshotOps.CacheConfig,
		b.snapshotOps.PatchFields,
		att.acceptedSnapshots,
		keyDeltas,
	)
	b.snapshotOps.Manager.Stage(snap)

	if err = b.commit(tx); err != nil {
		if b.sched.stats.Enabled {
			b.sched.stats.TxCommitErrors.Add(1)
		}
		b.snapshotOps.Manager.DropStaged(seq)
		assignPreparedErr(att.accepted, err)
		return nil, true, nil
	}

	name := opName(active[0].op, len(active), atomicAll, b.sched.maxOps)
	err = b.publishCommitted(seq, name, snap)
	if err != nil {
		assignPreparedErr(att.accepted, err)
	}

	return nil, true, nil
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
	st.borrowedPayload = nil
}

func (st *recordState) setBorrowedPayload(payload []byte) {
	st.ownedPayload = nil
	st.payloadOff = 0
	st.borrowedPayload = payload
}

func (st *recordState) clearPayload() {
	st.ownedPayload = nil
	st.payloadOff = 0
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
