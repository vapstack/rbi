package wexec

import (
	"fmt"
	"slices"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"go.etcd.io/bbolt"
)

type CollectionWriteAttempt struct {
	ex      *Executor
	bucket  *bbolt.Bucket
	att     *attemptState
	cur     *attemptState
	batches []Batch
	curB    []Batch
}

func (b *Executor) NewFrameAttempt(tx *bbolt.Tx, capHint int) (CollectionWriteAttempt, error) {
	bucket := tx.Bucket(b.dataBucket)
	if bucket == nil {
		if b.stats.Enabled {
			b.stats.TxOpErrors.Add(1)
		}
		return CollectionWriteAttempt{}, ErrBucketMissing
	}
	bucket.FillPercent = b.bucketFillPercent
	var stringMap *bbolt.Bucket
	if b.strKey {
		stringMap = tx.Bucket(b.strmapBucket)
		if stringMap == nil {
			if b.stats.Enabled {
				b.stats.TxOpErrors.Add(1)
			}
			return CollectionWriteAttempt{}, ErrStringMapBucketMissing
		}
	}

	att, _ := b.newAttemptState(bucket, stringMap, capHint, false)
	return CollectionWriteAttempt{
		ex:      b,
		bucket:  bucket,
		att:     att,
		batches: batchSlicePool.Get(1)[:0],
	}, nil
}

func (attempt *CollectionWriteAttempt) Prepare(batch *Batch, hookTx unsafe.Pointer, hookInsert *int) (int, error) {
	// Prepare takes ownership of the batch prefix; if hooks append generated
	// writes, the unprepared suffix is handed back as a new caller-owned batch.
	owned := *batch
	*batch = Batch{}
	active := owned.reqs
	if len(active) == 0 {
		owned.Cancel()
		return 0, nil
	}
	if attempt.cur == nil {
		attempt.beginCurrent(len(active))
	}
	if attempt.curB == nil {
		attempt.curB = batchSlicePool.Get(1)[:0]
	}

	n := len(active)
	if hookInsert == nil {
		attempt.ex.prepareActive(attempt.cur, active, hookTx)
	} else {
		for n = 0; n < len(active); n++ {
			req := &active[n]
			before := *hookInsert
			checkGenerated := len(req.onChange) != 0 && req.Err == nil
			if req.Err == nil {
				attempt.ex.prepareRequest(attempt.cur, req, hookTx)
			}
			if checkGenerated && *hookInsert != before {
				// Stop after the hook-triggering request so the caller can splice
				// generated writes before the remaining original requests.
				n++
				break
			}
		}
	}
	if err := firstRequestErr(active); err != nil {
		owned.Cancel()
		return 0, err
	}

	tailN := len(active) - n
	if tailN != 0 {
		// The accepted prefix stays in owned; move the suffix out so owned.Cancel
		// and frame cleanup cannot release the same requests.
		tail := requestScratchPool.Get(tailN)
		tail = append(tail, active[n:]...)
		clear(active[n:])
		tailHooks := false
		if owned.hooks {
			for i := range tail {
				if len(tail[i].onChange) != 0 {
					tailHooks = true
					break
				}
			}
		}
		*batch = Batch{ex: owned.ex, reqs: tail, hooks: tailHooks}
		owned.reqs = owned.reqs[:n]
	}
	attempt.curB = append(attempt.curB, owned)
	return tailN, nil
}

func (attempt *CollectionWriteAttempt) beginCurrent(capHint int) {
	cur, _ := attempt.ex.newAttemptState(attempt.bucket, attempt.att.strmapBucket, capHint, false)
	// cur overlays accepted state so one logical unit can be validated or
	// discarded without mutating earlier units in the same root frame.
	cur.base = attempt.att
	attempt.cur = cur
}

func (attempt *CollectionWriteAttempt) ValidateCurrent() error {
	cur := attempt.cur
	if cur == nil || len(cur.prepared) == 0 || attempt.ex.unique.Schema == nil || len(attempt.ex.unique.Schema.Unique) == 0 {
		return nil
	}
	n := len(cur.prepared)
	if cap(cur.uniqueIdxs) < n {
		cur.uniqueIdxs = slices.Grow(cur.uniqueIdxs, n)
	}
	if cap(cur.uniqueOldVals) < n {
		cur.uniqueOldVals = slices.Grow(cur.uniqueOldVals, n)
	}
	if cap(cur.uniqueNewVals) < n {
		cur.uniqueNewVals = slices.Grow(cur.uniqueNewVals, n)
	}
	cur.uniqueIdxs = cur.uniqueIdxs[:n]
	cur.uniqueOldVals = cur.uniqueOldVals[:n]
	cur.uniqueNewVals = cur.uniqueNewVals[:n]
	for i := range cur.prepared {
		op := cur.prepared[i]
		cur.uniqueIdxs[i] = op.idx
		cur.uniqueOldVals[i] = op.oldVal
		cur.uniqueNewVals[i] = op.newVal
	}
	cur.uniqueState.base = &attempt.att.uniqueState
	if err := attempt.ex.unique.checkOnWriteMulti(&cur.uniqueState, cur.uniqueIdxs, cur.uniqueOldVals, cur.uniqueNewVals); err != nil {
		if attempt.ex.stats.Enabled {
			attempt.ex.stats.UniqueRejected.Add(1)
		}
		assignPreparedErr(cur.prepared, err)
		return err
	}
	return nil
}

func (attempt *CollectionWriteAttempt) AcceptValidatedCurrent() {
	cur := attempt.cur
	if cur == nil {
		return
	}
	att := attempt.att
	if attempt.ex.unique.Schema != nil && len(attempt.ex.unique.Schema.Unique) != 0 {
		attempt.ex.unique.collectUniqueBatchDepartures(&cur.uniqueState, cur.uniqueIdxs, cur.uniqueOldVals, cur.uniqueNewVals)
	}
	att.uniqueState.mergeFrom(&cur.uniqueState)
	// Ownership of decoded values and encode buffers moves from cur to att;
	// clear cur's slices before pooling it so cleanup does not release twice.
	att.prepared = append(att.prepared, cur.prepared...)
	att.preparedSnapshots = append(att.preparedSnapshots, cur.preparedSnapshots...)
	att.ownedBuffers = append(att.ownedBuffers, cur.ownedBuffers...)
	att.releaseValues = append(att.releaseValues, cur.releaseValues...)
	attempt.mergeCurrentStates()
	attempt.batches = append(attempt.batches, attempt.curB...)
	batchSlicePool.Put(attempt.curB)

	clear(cur.ownedBuffers)
	cur.ownedBuffers = cur.ownedBuffers[:0]
	clear(cur.releaseValues)
	cur.releaseValues = cur.releaseValues[:0]
	attempt.curB = nil
	attempt.cur = nil
	attemptStatePool.Put(cur)
}

func (attempt *CollectionWriteAttempt) mergeCurrentStates() {
	att := attempt.att
	if attempt.ex.strKey {
		for key, pos := range attempt.cur.stateByStringID {
			dstPos, ok := att.stateByStringID[key]
			if !ok {
				dstPos = len(att.states)
				att.states = append(att.states, recordState{})
				att.stateByStringID[key] = dstPos
			}
			att.states[dstPos] = attempt.cur.states[pos]
			att.states[dstPos].key = keycodec.StringBytes(key)
		}
		return
	}
	for id, pos := range attempt.cur.stateByUintID {
		dstPos, ok := att.stateByUintID[id]
		if !ok {
			dstPos = len(att.states)
			att.states = append(att.states, recordState{})
			att.stateByUintID[id] = dstPos
		}
		att.states[dstPos] = attempt.cur.states[pos]
		att.states[dstPos].key = keycodec.U64BytesWithBuf(id, &att.states[dstPos].keyBuf)
	}
}

func (attempt *CollectionWriteAttempt) DiscardCurrent() {
	if attempt.cur != nil {
		attemptStatePool.Put(attempt.cur)
		attempt.cur = nil
	}
	for i := range attempt.curB {
		attempt.curB[i].Cancel()
	}
	if attempt.curB != nil {
		batchSlicePool.Put(attempt.curB)
	}
	attempt.curB = nil
}

func (attempt *CollectionWriteAttempt) Apply() (AppliedBatch, error) {
	att := attempt.att
	if att == nil {
		return AppliedBatch{}, nil
	}
	if len(att.prepared) == 0 {
		attempt.consumeBatches()
		attemptStatePool.Put(att)
		attempt.att = nil
		return AppliedBatch{}, nil
	}
	att.accepted = att.prepared
	att.acceptedSnapshots = att.preparedSnapshots
	if err := attempt.ex.applyAccepted(att); err != nil {
		attempt.consumeBatches()
		attemptStatePool.Put(att)
		attempt.att = nil
		return AppliedBatch{}, err
	}

	seq, err := attempt.bucket.NextSequence()
	if err != nil {
		if attempt.ex.stats.Enabled {
			attempt.ex.stats.TxOpErrors.Add(1)
		}
		attempt.consumeBatches()
		attemptStatePool.Put(att)
		attempt.att = nil
		return AppliedBatch{}, fmt.Errorf("%w: %w", ErrAdvanceBucketSequence, err)
	}

	// The applied batch keeps attempt state and accepted request batches alive
	// until the root commit path publishes or discards the generated snapshot.
	cleanup := appliedBatchCleanupPool.Get()
	cleanup.att = att
	cleanup.batches = attempt.batches
	applied := AppliedBatch{Seq: seq, cleanup: cleanup}
	attempt.att = nil
	attempt.batches = nil
	if !attempt.ex.snapshotsEnabled() {
		return applied, nil
	}
	applied.Snapshot = attempt.ex.buildAcceptedSnapshot(seq, att)
	return applied, nil
}

func (attempt *CollectionWriteAttempt) Cancel() {
	attempt.DiscardCurrent()
	if attempt.att != nil {
		attemptStatePool.Put(attempt.att)
		attempt.att = nil
	}
	attempt.consumeBatches()
	attempt.bucket = nil
}

func (attempt *CollectionWriteAttempt) consumeBatches() {
	for i := range attempt.batches {
		attempt.batches[i].Cancel()
	}
	if attempt.batches != nil {
		batchSlicePool.Put(attempt.batches)
	}
	attempt.batches = nil
}
