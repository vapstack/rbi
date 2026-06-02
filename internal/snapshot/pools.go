package snapshot

import "github.com/vapstack/pooled"

const uint64IntMapPoolMaxLen = 16 << 10

var snapshotRefPool = pooled.Pointers[Ref]{Clear: true}

var snapshotUniverseOwnerPool = pooled.Pointers[universeOwner]{Clear: true}

var snapshotRetiredListPool = pooled.Slices[*View]{MaxCap: 64, Clear: pooled.ClearCap}

var uint64IntMapPool = pooled.Maps[uint64, int]{
	NewCap: 256,
	MaxLen: uint64IntMapPoolMaxLen,
}
