package wexec

import (
	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/keycodec"
)

const requestScratchPoolCap = 4 << 10

var encodePool pooled.Buffers

var requestScratchPool = pooled.Slices[*request]{MaxCap: requestScratchPoolCap, Clear: pooled.ClearCap}

var requestPool = pooled.Pointers[request]{
	Init: func(req *request) {
		if req.Done == nil {
			req.Done = make(chan error, 1)
		}
	},
	Cleanup: func(req *request) {
		if req.setPayload != nil {
			encodePool.Put(req.setPayload)
			req.setPayload = nil
		}
		req.setValue = nil
		req.setBaseline = nil
		clear(req.patch)
		req.patch = req.patch[:0]
		req.patchIgnoreUnknown = false
		req.beforeProcess = nil
		req.beforeStore = nil
		req.beforeCommit = nil
		req.cloneValue = nil
		req.policy = 0
		req.replacedBy = nil
		select {
		case <-req.Done:
		default:
		}
		req.op = 0
		req.id = keycodec.DataKey{}
		req.Err = nil
	},
}

var jobPool = pooled.Pointers[writeJob]{
	Init: func(job *writeJob) {
		if job.done == nil {
			job.done = make(chan error, 1)
		}
	},
	Cleanup: func(job *writeJob) {
		select {
		case <-job.done:
		default:
		}
		job.reqs = nil
		job.isolated = false
		job.enqueuedAt = 0
	},
}

var attemptStatePool = pooled.Pointers[attemptState]{
	Cleanup: func(st *attemptState) {
		st.cleanup()

		clear(st.prepared)
		st.prepared = st.prepared[:0]

		clear(st.accepted)
		st.accepted = st.accepted[:0]

		clear(st.states)
		st.states = st.states[:0]

		if st.stateByUintID != nil {
			clear(st.stateByUintID)
		}
		if st.stateByStringID != nil {
			clear(st.stateByStringID)
		}
	},
}

var repeatUintIDPool = pooled.Maps[uint64, int]{
	NewCap: 256,
}

var repeatStringIDPool = pooled.Maps[string, int]{
	NewCap: 256,
}
