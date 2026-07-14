package wexec

import (
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

var errEmptyPayload = errors.New("empty record payload")

const stringValuePrefixLen = 8

type request struct {
	op Op
	id keycodec.DataKey

	setValue   unsafe.Pointer
	setBuffer  []byte
	payloadOff uint8

	patch []schema.PatchItem

	patchIgnoreUnknown bool
	generationDepth    uint8

	onChange []OnChangeHook

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

func (batch *Batch) AddSet(key keycodec.DataKey, newVal unsafe.Pointer, onChange []OnChangeHook, depth uint8) error {
	req, err := batch.ex.buildSetRequest(key, newVal, onChange, depth)
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

func (b *Executor) buildSetRequest(key keycodec.DataKey, newVal unsafe.Pointer, onChange []OnChangeHook, depth uint8) (request, error) {
	var req request
	req.op = opSet
	req.id = key
	req.onChange = onChange
	req.generationDepth = depth

	if b.setNeedsValue(onChange) {
		req.setValue = b.ops.Acquire()
		if err := b.ops.CloneInto(newVal, req.setValue); err != nil {
			b.ops.Release(req.setValue)
			return request{}, formatPrepareErr(prepareErrClone, err)
		}
	}

	if len(onChange) == 0 {
		buf := encodeBufferPool.Get()
		buf, req.payloadOff = reserveStringValuePrefix(buf, b.strKey)
		var err error
		buf, err = b.ops.Encode(newVal, buf)
		if err != nil {
			encodeBufferPool.Put(buf)
			if req.setValue != nil {
				b.ops.Release(req.setValue)
			}
			return request{}, formatPrepareErr(prepareErrEncode, err)
		}
		if len(buf) == int(req.payloadOff) {
			encodeBufferPool.Put(buf)
			if req.setValue != nil {
				b.ops.Release(req.setValue)
			}
			return request{}, errEmptyPayload
		}
		req.setBuffer = buf
	}

	return req, nil
}

func (b *Executor) setNeedsValue(onChange []OnChangeHook) bool {
	return len(onChange) != 0 ||
		b.indexed && b.schema.HasQueryFields() ||
		b.unique.Schema != nil && len(b.unique.Schema.Unique) != 0
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
	if req.setBuffer == nil {
		return nil
	}
	return req.setBuffer[req.payloadOff:]
}

func (b *Executor) releaseRequest(req *request) {
	if req.setValue != nil {
		b.ops.Release(req.setValue)
		req.setValue = nil
	}
	if req.setBuffer != nil {
		encodeBufferPool.Put(req.setBuffer)
		req.setBuffer = nil
	}
	req.payloadOff = 0
	schema.ReleasePatchItemSlice(req.patch)
	req.patch = nil
	req.patchIgnoreUnknown = false
	req.generationDepth = 0
	req.onChange = nil
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
