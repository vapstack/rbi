package snapshot

import "github.com/vapstack/rbi/internal/pooled"

const uint64IntMapPoolMaxLen = 16 << 10

var snapshotRefPool = pooled.Pointers[Ref]{Clear: true}

var snapshotUniverseOwnerPool = pooled.Pointers[universeOwner]{Clear: true}

var snapshotRetiredListPool = pooled.NewSlicePool[*View](64, pooled.ClearCap)

var uint64IntMapPool = pooled.Maps[uint64, int]{
	NewCap: 8,
	MaxLen: uint64IntMapPoolMaxLen,
}
