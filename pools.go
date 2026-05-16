package rbi

import (
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const uint64IntMapPoolMaxLen = 16 << 10

/**/

var uint64IntMapPool = pooled.Maps[uint64, int]{
	NewCap: 8,
	MaxLen: uint64IntMapPoolMaxLen,
	Cleanup: func(m map[uint64]int) {
		clear(m)
	},
}

var (
	uniqueLeavingOuterPool = pooled.Maps[string, map[keycodec.IndexLookupKey]posting.List]{
		NewCap: 8,
		MaxLen: 256,
		Cleanup: func(m map[string]map[keycodec.IndexLookupKey]posting.List) {
			for _, inner := range m {
				uniqueLeavingInnerPool.Put(inner)
			}
			clear(m)
		},
	}
	uniqueLeavingInnerPool = pooled.Maps[keycodec.IndexLookupKey, posting.List]{
		NewCap: 8,
		MaxLen: 4 << 10,
		Cleanup: func(m map[keycodec.IndexLookupKey]posting.List) {
			for _, ids := range m {
				ids.Release()
			}
			clear(m)
		},
	}
	uniqueSeenOuterPool = pooled.Maps[string, map[keycodec.IndexLookupKey]uint64]{
		NewCap: 8,
		MaxLen: 256,
		Cleanup: func(m map[string]map[keycodec.IndexLookupKey]uint64) {
			for _, inner := range m {
				uniqueSeenInnerPool.Put(inner)
			}
			clear(m)
		},
	}
	uniqueSeenInnerPool = pooled.Maps[keycodec.IndexLookupKey, uint64]{
		NewCap: 8,
		MaxLen: 4 << 10,
		Cleanup: func(m map[keycodec.IndexLookupKey]uint64) {
			clear(m)
		},
	}
)
