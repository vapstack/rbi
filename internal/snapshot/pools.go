package snapshot

import "github.com/vapstack/pooled"

const (
	uint64IntMapPoolMaxLen    = 16 << 10
	batchEntrySlicePoolMaxCap = 16 << 10
)

var snapshotUniverseOwnerPool = pooled.Pointers[universeOwner]{Clear: true}

var batchEntrySlicePool = pooled.Slices[BatchEntry]{MaxCap: batchEntrySlicePoolMaxCap, Clear: pooled.ClearCap}

var uint64IntMapPool = pooled.Maps[uint64, int]{
	NewCap: 256,
	MaxLen: uint64IntMapPoolMaxLen,
}

type keyDeltaStateKey struct {
	Key string
	ID  uint64
}

var keyDeltaStateMapPool = pooled.Maps[keyDeltaStateKey, bool]{
	NewCap: 64,
	MaxLen: 4 << 10,
}

var keyPostingDeltaMapPool = pooled.Maps[string, uint32]{
	NewCap: 64,
	MaxLen: 4 << 10,
}
