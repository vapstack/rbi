package wexec

import "github.com/vapstack/pooled"

const requestScratchPoolCap = 4 << 10

var encodePool = pooled.Buffers{
	MaxCap: 512 << 10,
}

var requestScratchPool = pooled.Slices[request]{MaxCap: requestScratchPoolCap, Clear: pooled.ClearLen}

var batchSlicePool = pooled.Slices[Batch]{MaxCap: requestScratchPoolCap, Clear: pooled.ClearCap}

var attemptStatePool = pooled.Pointers[attemptState]{
	Cleanup: func(st *attemptState) {
		st.cleanup()

		clear(st.prepared)
		st.prepared = st.prepared[:0]

		clear(st.accepted)
		st.accepted = st.accepted[:0]

		clear(st.states)
		st.states = st.states[:0]

		if len(st.stateByUintID) < 1024 {
			clear(st.stateByUintID)
		} else {
			st.stateByUintID = nil
		}
		if len(st.stateByStringID) < 1024 {
			clear(st.stateByStringID)
		} else {
			st.stateByStringID = nil
		}
	},
}

var appliedBatchCleanupPool = pooled.Pointers[appliedBatchCleanup]{
	Cleanup: func(cleanup *appliedBatchCleanup) {
		cleanup.att = nil
		cleanup.ex = nil
		cleanup.reqs = nil
		cleanup.batches = nil
	},
}
