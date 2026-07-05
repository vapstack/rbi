package wexec

import (
	"bytes"
	"errors"
	"fmt"
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

var errEmptyPayload = errors.New("empty msgpack payload")

const stringValuePrefixLen = 8

type request struct {
	op Op
	id keycodec.DataKey

	setBaseline unsafe.Pointer
	setPayload  *bytes.Buffer
	payloadOff  uint8

	patch []schema.PatchItem

	patchIgnoreUnknown bool
	generationDepth    uint8

	onChange   []OnChangeHook
	cloneValue CloneFunc

	Err error
}

type Batch struct {
	ex    *Executor
	reqs  []request
	hooks bool
}

func (b *Executor) NewBatch() Batch {
	reqs := requestScratchPool.Get(64)
	return Batch{
		ex:   b,
		reqs: reqs,
	}
}

func (batch *Batch) AddSet(key keycodec.DataKey, newVal unsafe.Pointer, onChange []OnChangeHook, cloneValue CloneFunc, depth uint8) error {
	req, err := batch.ex.buildSetRequest(key, newVal, onChange, cloneValue, depth)
	if err != nil {
		return err
	}
	if len(onChange) != 0 {
		batch.hooks = true
	}
	batch.reqs = append(batch.reqs, req)
	return nil
}

// AddPatch takes ownership of patch until the request is released.
func (batch *Batch) AddPatch(key keycodec.DataKey, patch []schema.PatchItem, ignoreUnknown bool, onChange []OnChangeHook, depth uint8) {
	if len(onChange) != 0 {
		batch.hooks = true
	}
	batch.reqs = append(batch.reqs, batch.ex.buildPatchRequest(key, patch, ignoreUnknown, onChange, depth))
}

func (batch *Batch) AddDelete(key keycodec.DataKey, onChange []OnChangeHook, depth uint8) {
	if len(onChange) != 0 {
		batch.hooks = true
	}
	batch.reqs = append(batch.reqs, batch.ex.buildDeleteRequest(key, onChange, depth))
}

func (batch *Batch) HasOnChange() bool {
	return batch.hooks
}

func (batch *Batch) Cancel() {
	reqs := batch.reqs
	batch.reqs = nil
	batch.hooks = false
	for i := 0; i < len(reqs); i++ {
		batch.ex.releaseRequest(&reqs[i])
	}
	requestScratchPool.Put(reqs)
}

func (b *Executor) buildSetRequest(key keycodec.DataKey, newVal unsafe.Pointer, onChange []OnChangeHook, cloneValue CloneFunc, depth uint8) (request, error) {
	var req request
	req.op = opSet
	req.id = key
	req.onChange = onChange
	req.cloneValue = cloneValue
	req.generationDepth = depth
	if len(onChange) != 0 && cloneValue != nil {
		var err error
		if req.setBaseline, err = cloneValue(req.id, newVal); err != nil {
			return request{}, err
		}
	} else {
		buf := encodePool.Get()
		req.payloadOff = reserveStringValuePrefix(buf, b.strKey)
		if err := b.ops.Encode(newVal, buf); err != nil {
			encodePool.Put(buf)
			return request{}, formatPrepareErr(prepareErrEncode, err)
		}
		if b.rejectEmptyPayload && buf.Len() == int(req.payloadOff) {
			encodePool.Put(buf)
			return request{}, formatPrepareErr(prepareErrEncode, errEmptyPayload)
		}
		req.setPayload = buf
	}
	return req, nil
}

// buildPatchRequest takes ownership of patch until releaseRequest.
func (b *Executor) buildPatchRequest(key keycodec.DataKey, patch []schema.PatchItem, ignoreUnknown bool, onChange []OnChangeHook, depth uint8) request {
	var req request
	req.op = opPatch
	req.id = key
	req.generationDepth = depth
	req.patch = patch
	req.patchIgnoreUnknown = ignoreUnknown
	req.onChange = onChange

	return req
}

func (b *Executor) buildDeleteRequest(key keycodec.DataKey, onChange []OnChangeHook, depth uint8) request {
	var req request
	req.op = opDelete
	req.id = key
	req.generationDepth = depth
	req.onChange = onChange
	return req
}

func (req *request) payloadBytes() []byte {
	if req.setPayload == nil {
		return nil
	}
	return req.setPayload.Bytes()[req.payloadOff:]
}

func (b *Executor) releaseRequest(req *request) {
	if req.setBaseline != nil {
		b.ops.Release(req.setBaseline)
		req.setBaseline = nil
	}
	if req.setPayload != nil {
		encodePool.Put(req.setPayload)
		req.setPayload = nil
	}
	req.payloadOff = 0
	schema.ReleasePatchItemSlice(req.patch)
	req.patch = nil
	req.patchIgnoreUnknown = false
	req.generationDepth = 0
	req.onChange = nil
	req.cloneValue = nil
	req.op = 0
	req.id = keycodec.DataKey{}
	req.Err = nil
}

func firstRequestErr(reqs []request) error {
	for i := 0; i < len(reqs); i++ {
		req := &reqs[i]
		if req.Err != nil {
			return req.Err
		}
	}
	return nil
}
