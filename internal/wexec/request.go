package wexec

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
)

type Op uint8

const (
	opSet Op = iota + 1
	opPatch
	opDelete
)

func (op Op) String() string {
	switch op {
	case opSet:
		return "set"
	case opPatch:
		return "patch"
	case opDelete:
		return "delete"
	default:
		return fmt.Sprintf("unknown(%d)", op)
	}
}

func opName(op Op, reqLen int, atomicAll bool, maxOps int) string {
	if reqLen == 0 {
		return "batch"
	}
	if !atomicAll {
		if reqLen != 1 || maxOps > 1 {
			return "batch"
		}
		switch op {
		case opSet:
			return "set"
		case opPatch:
			return "patch"
		case opDelete:
			return "delete"
		default:
			return "batch"
		}
	}
	if reqLen == 1 {
		switch op {
		case opSet:
			return "set"
		case opPatch:
			return "patch"
		case opDelete:
			return "delete"
		default:
			return "batch"
		}
	}
	switch op {
	case opSet:
		return "batch_set"
	case opPatch:
		return "batch_patch"
	case opDelete:
		return "batch_delete"
	default:
		return "batch"
	}
}

type reqPolicy uint8

const (
	reqRepeatIDSafeShared reqPolicy = 1 << iota
	reqSetDeleteCoalescible
)

var (
	errEmptyPayload = errors.New("empty msgpack payload")
	errUnknownOp    = errors.New("unknown auto-batch op")
)

type request struct {
	op Op
	id keycodec.DataKey

	setValue    unsafe.Pointer
	setBaseline unsafe.Pointer
	setPayload  *bytes.Buffer

	patch []schema.PatchItem

	patchIgnoreUnknown bool

	beforeProcess []BeforeProcessHook
	beforeStore   []BeforeStoreHook
	beforeCommit  []BeforeCommitHook
	cloneValue    CloneFunc
	policy        reqPolicy

	replacedBy *request

	Err  error
	Done chan error
}

type writeJob struct {
	reqs       []*request
	isolated   bool
	done       chan error
	enqueuedAt int64
}

type Batch struct {
	ex   *Batcher
	reqs []*request
	n    int
}

func (b *Batcher) NewBatch(capHint int) Batch {
	reqs := requestScratchPool.Get(capHint)[:capHint]
	return Batch{
		ex:   b,
		reqs: reqs,
	}
}

func (batch *Batch) AddSet(key keycodec.DataKey, newVal unsafe.Pointer, beforeStore []BeforeStoreHook, beforeCommit []BeforeCommitHook, cloneValue CloneFunc) error {
	req, err := batch.ex.buildSetRequest(key, newVal, beforeStore, beforeCommit, cloneValue)
	if err != nil {
		return err
	}
	batch.reqs[batch.n] = req
	batch.n++
	return nil
}

func (batch *Batch) AddPatch(key keycodec.DataKey, patch []schema.PatchItem, ignoreUnknown bool, beforeProcess []BeforeProcessHook, beforeStore []BeforeStoreHook, beforeCommit []BeforeCommitHook) {
	batch.reqs[batch.n] = batch.ex.buildPatchRequest(key, patch, ignoreUnknown, beforeProcess, beforeStore, beforeCommit)
	batch.n++
}

func (batch *Batch) AddDelete(key keycodec.DataKey, beforeCommit []BeforeCommitHook) {
	batch.reqs[batch.n] = batch.ex.buildDeleteRequest(key, beforeCommit)
	batch.n++
}

func (batch *Batch) Submit(isolated bool) error {
	reqs := batch.reqs[:batch.n]
	batch.reqs = nil
	batch.n = 0
	err := batch.ex.submit(reqs, isolated)
	requestScratchPool.Put(reqs)
	return err
}

func (batch *Batch) Cancel() {
	reqs := batch.reqs[:batch.n]
	batch.reqs = nil
	batch.n = 0
	for i := 0; i < len(reqs); i++ {
		requestPool.Put(reqs[i])
	}
	requestScratchPool.Put(reqs)
}

func (b *Batcher) buildSetRequest(key keycodec.DataKey, newVal unsafe.Pointer, beforeStore []BeforeStoreHook, beforeCommit []BeforeCommitHook, cloneValue CloneFunc) (*request, error) {
	req := requestPool.Get()
	req.op = opSet
	req.id = key
	req.beforeStore = beforeStore
	req.beforeCommit = beforeCommit
	req.cloneValue = cloneValue
	if len(beforeStore) > 0 && cloneValue != nil {
		var err error
		if req.setBaseline, err = cloneValue(req.id, newVal); err != nil {
			requestPool.Put(req)
			return nil, err
		}
	} else {
		buf := encodePool.Get()
		if err := b.ops.Encode(newVal, buf); err != nil {
			encodePool.Put(buf)
			requestPool.Put(req)
			return nil, formatPrepareErr(prepareErrEncode, err)
		}
		req.setValue = newVal
		req.setPayload = buf
	}
	if len(beforeStore) == 0 && len(beforeCommit) == 0 {
		req.policy = reqSetDeleteCoalescible
	}
	return req, nil
}

func (b *Batcher) buildPatchRequest(key keycodec.DataKey, patch []schema.PatchItem, ignoreUnknown bool, beforeProcess []BeforeProcessHook, beforeStore []BeforeStoreHook, beforeCommit []BeforeCommitHook) *request {
	req := requestPool.Get()
	req.op = opPatch
	req.id = key
	if cap(req.patch) < len(patch) {
		req.patch = slices.Grow(req.patch, len(patch))
	}
	req.patch = req.patch[:len(patch)]
	copy(req.patch, patch)

	req.patchIgnoreUnknown = ignoreUnknown
	req.beforeProcess = beforeProcess
	req.beforeStore = beforeStore
	req.beforeCommit = beforeCommit

	rt := b.schema

	if !b.indexed || !rt.HasUnique {
		req.policy = reqRepeatIDSafeShared

	} else if len(beforeProcess) == 0 && len(beforeStore) == 0 {
		touchesUnique := false
		for i := range patch {
			if rt.PatchNameTouchesUnique(patch[i].Name) {
				touchesUnique = true
				break
			}
		}
		if !touchesUnique {
			req.policy = reqRepeatIDSafeShared
		}
	}
	return req
}

func (b *Batcher) buildDeleteRequest(key keycodec.DataKey, beforeCommit []BeforeCommitHook) *request {
	req := requestPool.Get()
	req.op = opDelete
	req.id = key
	req.beforeCommit = beforeCommit
	req.policy = reqRepeatIDSafeShared
	if len(beforeCommit) == 0 {
		req.policy |= reqSetDeleteCoalescible
	}
	return req
}

func (req *request) payloadBytes() []byte {
	if req.setPayload == nil {
		return nil
	}
	return req.setPayload.Bytes()
}

func (req *request) hasPolicy(policy reqPolicy) bool {
	return req.policy&policy != 0
}

func (req *request) canCoalesceSetDelete() bool {
	return req.replacedBy == nil && req.hasPolicy(reqSetDeleteCoalescible)
}

func (req *request) canShareRepeatedID() bool {
	return req.replacedBy == nil && req.hasPolicy(reqRepeatIDSafeShared)
}

func assignRequestErr(reqs []*request, err error) {
	for i := 0; i < len(reqs); i++ {
		req := reqs[i]
		if req.Err == nil {
			req.Err = err
		}
	}
}

func firstRequestErr(reqs []*request) error {
	for i := 0; i < len(reqs); i++ {
		req := reqs[i]
		if req.Err != nil {
			return req.Err
		}
	}
	return nil
}

func resolveRequestErrs(reqs []*request) {
	for i := 0; i < len(reqs); i++ {
		req := reqs[i]
		if req.replacedBy == nil {
			continue
		}
		target := req.replacedBy
		for target.replacedBy != nil {
			target = target.replacedBy
		}
		req.Err = target.Err
	}
}

func finishJobs(batch []*writeJob) {
	for _, job := range batch {
		resolveRequestErrs(job.reqs)
	}
	for _, job := range batch {
		var err error
		if len(job.reqs) == 1 {
			err = job.reqs[0].Err
		} else {
			err = firstRequestErr(job.reqs)
		}
		job.done <- err
	}
}

func failJobs(batch []*writeJob, err error) {
	for _, job := range batch {
		assignRequestErr(job.reqs, err)
	}
	finishJobs(batch)
}
