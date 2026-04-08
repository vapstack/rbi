package rbi

import (
	"slices"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

type batchPostingAccum struct {
	add    insertPostingAccum
	remove insertPostingAccum
}

type batchPostingAccumArena struct{ values []batchPostingAccum }

var (
	batchPostingAccumArenaPool = pooled.Pointers[batchPostingAccumArena]{
		New: func() *batchPostingAccumArena {
			return &batchPostingAccumArena{values: make([]batchPostingAccum, 0, 8)}
		},
	}
	batchPostingAccumMapPool = pooled.Maps[string, uint32]{
		NewCap: 8,
		MaxLen: batchPostingDeltaMapPoolMaxLen,
		Cleanup: func(m map[string]uint32) {
			clear(m)
		},
	}
	fixedBatchPostingAccumMapPool = pooled.Maps[uint64, uint32]{
		NewCap: 8,
		MaxLen: batchPostingDeltaMapPoolMaxLen,
		Cleanup: func(m map[uint64]uint32) {
			clear(m)
		},
	}
)

func getBatchPostingAccumArena(capHint int) *batchPostingAccumArena {
	arena := batchPostingAccumArenaPool.Get()
	if cap(arena.values) < capHint {
		arena.values = slices.Grow(arena.values, capHint)
	}
	return arena
}

func releaseBatchPostingAccumArena(arena *batchPostingAccumArena) {
	if arena == nil {
		return
	}
	for i := range arena.values {
		resetBatchPostingAccum(&arena.values[i])
	}
	if cap(arena.values) > batchPostingDeltaMapPoolMaxLen {
		arena.values = arena.values[:0]
		return
	}
	arena.values = arena.values[:0]
	batchPostingAccumArenaPool.Put(arena)
}

func (arena *batchPostingAccumArena) alloc() uint32 {
	ref := uint32(len(arena.values))
	arena.values = append(arena.values, batchPostingAccum{})
	return ref
}

func (arena *batchPostingAccumArena) accum(ref uint32) *batchPostingAccum {
	return &arena.values[ref]
}

func resetBatchPostingAccum(acc *batchPostingAccum) {
	resetInsertPostingAccum(&acc.add)
	resetInsertPostingAccum(&acc.remove)
}

func (acc *batchPostingAccum) addID(id uint64, isAdd bool) {
	if isAdd {
		acc.add.add(id)
		return
	}
	acc.remove.add(id)
}

func (acc *batchPostingAccum) materializeOwned() batchPostingDelta {
	return batchPostingDelta{
		add:    acc.add.materializeOwned(),
		remove: acc.remove.materializeOwned(),
	}
}

func addFieldBatchPostingAccum(
	fieldDelta map[string]uint32,
	arena **batchPostingAccumArena,
	key string,
	idx uint64,
	isAdd bool,
	capHint int,
) map[string]uint32 {
	if fieldDelta == nil {
		if capHint >= 64 {
			fieldDelta = make(map[string]uint32, min(capHint, batchPostingDeltaMapPoolMaxLen))
		} else {
			fieldDelta = batchPostingAccumMapPool.Get()
		}
	}
	ref, ok := fieldDelta[key]
	if !ok {
		if *arena == nil {
			*arena = getBatchPostingAccumArena(capHint)
		}
		ref = (*arena).alloc()
		fieldDelta[key] = ref
	}
	(*arena).accum(ref).addID(idx, isAdd)
	return fieldDelta
}

func addFixedFieldBatchPostingAccum(
	fieldDelta map[uint64]uint32,
	arena **batchPostingAccumArena,
	key uint64,
	idx uint64,
	isAdd bool,
	capHint int,
) map[uint64]uint32 {
	if fieldDelta == nil {
		if capHint >= 64 {
			fieldDelta = make(map[uint64]uint32, min(capHint, batchPostingDeltaMapPoolMaxLen))
		} else {
			fieldDelta = fixedBatchPostingAccumMapPool.Get()
		}
	}
	ref, ok := fieldDelta[key]
	if !ok {
		if *arena == nil {
			*arena = getBatchPostingAccumArena(capHint)
		}
		ref = (*arena).alloc()
		fieldDelta[key] = ref
	}
	(*arena).accum(ref).addID(idx, isAdd)
	return fieldDelta
}

/**/

const insertPostingIDsBufPoolMinCap = posting.MidCap

type insertPostingIDsBuf struct{ values []uint64 }

var insertPostingIDsBufPool = pooled.Pointers[insertPostingIDsBuf]{
	New: func() *insertPostingIDsBuf {
		return &insertPostingIDsBuf{
			values: make([]uint64, 0, insertPostingIDsBufPoolMinCap),
		}
	},
}

func getInsertPostingIDsBuf(capHint int) *insertPostingIDsBuf {
	buf := insertPostingIDsBufPool.Get()
	if cap(buf.values) < capHint {
		buf.values = slices.Grow(buf.values, capHint)
	}
	return buf
}

func releaseInsertPostingIDsBuf(buf *insertPostingIDsBuf) {
	if buf == nil {
		return
	}
	if cap(buf.values) > uint64SlicePoolMaxCap {
		return
	}
	buf.values = buf.values[:0]
	insertPostingIDsBufPool.Put(buf)
}

type insertPostingAccum struct {
	spill     *insertPostingIDsBuf
	inlineLen uint8
	inline    [posting.SmallCap]uint64
}

type insertPostingAccumArena struct{ values []insertPostingAccum }

var (
	insertPostingAccumArenaPool = pooled.Pointers[insertPostingAccumArena]{
		New: func() *insertPostingAccumArena {
			return &insertPostingAccumArena{values: make([]insertPostingAccum, 0, 8)}
		},
	}
	insertPostingMapPool = pooled.Maps[string, uint32]{
		NewCap: 8,
		MaxLen: postingMapPoolMaxLen,
		Cleanup: func(m map[string]uint32) {
			clear(m)
		},
	}
	fixedInsertPostingMapPool = pooled.Maps[uint64, uint32]{
		NewCap: 8,
		MaxLen: postingMapPoolMaxLen,
		Cleanup: func(m map[uint64]uint32) {
			clear(m)
		},
	}
)

func getInsertPostingAccumArena(capHint int) *insertPostingAccumArena {
	arena := insertPostingAccumArenaPool.Get()
	if cap(arena.values) < capHint {
		arena.values = slices.Grow(arena.values, capHint)
	}
	return arena
}

func releaseInsertPostingAccumArena(arena *insertPostingAccumArena) {
	if arena == nil {
		return
	}
	for i := range arena.values {
		resetInsertPostingAccum(&arena.values[i])
	}
	if cap(arena.values) > postingMapPoolMaxLen {
		arena.values = arena.values[:0]
		return
	}
	arena.values = arena.values[:0]
	insertPostingAccumArenaPool.Put(arena)
}

func (arena *insertPostingAccumArena) alloc() uint32 {
	ref := uint32(len(arena.values))
	arena.values = append(arena.values, insertPostingAccum{})
	return ref
}

func (arena *insertPostingAccumArena) accum(ref uint32) *insertPostingAccum {
	return &arena.values[ref]
}

func resetInsertPostingAccum(acc *insertPostingAccum) {
	releaseInsertPostingIDsBuf(acc.spill)
	acc.spill = nil
	acc.inlineLen = 0
}

func (acc *insertPostingAccum) add(id uint64) {
	if acc.spill != nil {
		acc.spill.values = append(acc.spill.values, id)
		return
	}
	n := int(acc.inlineLen)
	if n < len(acc.inline) {
		acc.inline[n] = id
		acc.inlineLen++
		return
	}

	buf := getInsertPostingIDsBuf(posting.MidCap)
	buf.values = append(buf.values, acc.inline[:]...)
	buf.values = append(buf.values, id)
	acc.spill = buf
	acc.inlineLen = 0
}

func compactSortedUint64s(ids []uint64) []uint64 {
	if len(ids) < 2 {
		return ids
	}
	write := 1
	prev := ids[0]
	for i := 1; i < len(ids); i++ {
		if ids[i] == prev {
			continue
		}
		ids[write] = ids[i]
		prev = ids[i]
		write++
	}
	return ids[:write]
}

func materializeInsertPostingIDsOwned(ids []uint64) posting.List {
	if len(ids) == 0 {
		return posting.List{}
	}
	if len(ids) > 1 {
		slices.Sort(ids)
		ids = compactSortedUint64s(ids)
	}
	return posting.BuildFromSorted(ids)
}

func (acc *insertPostingAccum) materializeOwned() posting.List {
	if acc == nil {
		return posting.List{}
	}
	if acc.spill != nil {
		out := materializeInsertPostingIDsOwned(acc.spill.values)
		releaseInsertPostingIDsBuf(acc.spill)
		acc.spill = nil
		return out
	}
	n := int(acc.inlineLen)
	acc.inlineLen = 0
	return materializeInsertPostingIDsOwned(acc.inline[:n])
}

func addInsertPostingAccum(
	fieldMap map[string]uint32,
	arena **insertPostingAccumArena,
	key string,
	idx uint64,
	capHint int,
) map[string]uint32 {
	if fieldMap == nil {
		if capHint >= 64 {
			fieldMap = make(map[string]uint32, min(capHint, postingMapPoolMaxLen))
		} else {
			fieldMap = insertPostingMapPool.Get()
		}
	}
	ref, ok := fieldMap[key]
	if !ok {
		if *arena == nil {
			*arena = getInsertPostingAccumArena(capHint)
		}
		ref = (*arena).alloc()
		fieldMap[key] = ref
	}
	(*arena).accum(ref).add(idx)
	return fieldMap
}

func addFixedInsertPostingAccum(
	fieldMap map[uint64]uint32,
	arena **insertPostingAccumArena,
	key uint64,
	idx uint64,
	capHint int,
) map[uint64]uint32 {
	if fieldMap == nil {
		if capHint >= 64 {
			fieldMap = make(map[uint64]uint32, min(capHint, postingMapPoolMaxLen))
		} else {
			fieldMap = fixedInsertPostingMapPool.Get()
		}
	}
	ref, ok := fieldMap[key]
	if !ok {
		if *arena == nil {
			*arena = getInsertPostingAccumArena(capHint)
		}
		ref = (*arena).alloc()
		fieldMap[key] = ref
	}
	(*arena).accum(ref).add(idx)
	return fieldMap
}
