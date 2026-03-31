package rbi

import (
	"slices"
	"sync"

	"github.com/vapstack/rbi/internal/posting"
)

type batchPostingAccum struct {
	add    insertPostingAccum
	remove insertPostingAccum
}

type batchPostingAccumArena struct{ values []batchPostingAccum }

var (
	batchPostingAccumArenaPool    sync.Pool
	batchPostingAccumMapPool      sync.Pool
	fixedBatchPostingAccumMapPool sync.Pool
)

func getBatchPostingAccumArena(capHint int) *batchPostingAccumArena {
	if v := batchPostingAccumArenaPool.Get(); v != nil {
		arena := v.(*batchPostingAccumArena)
		if cap(arena.values) < capHint {
			arena.values = slices.Grow(arena.values, capHint)
		}
		return arena
	}
	return &batchPostingAccumArena{values: make([]batchPostingAccum, 0, max(capHint, 8))}
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

func getBatchPostingAccumMap() map[string]uint32 {
	if v := batchPostingAccumMapPool.Get(); v != nil {
		return v.(map[string]uint32)
	}
	return make(map[string]uint32, 8)
}

func getBatchPostingAccumMapHint(capHint int) map[string]uint32 {
	if capHint >= 64 {
		return make(map[string]uint32, min(capHint, batchPostingDeltaMapPoolMaxLen))
	}
	return getBatchPostingAccumMap()
}

func getFixedBatchPostingAccumMap() map[uint64]uint32 {
	if v := fixedBatchPostingAccumMapPool.Get(); v != nil {
		return v.(map[uint64]uint32)
	}
	return make(map[uint64]uint32, 8)
}

func getFixedBatchPostingAccumMapHint(capHint int) map[uint64]uint32 {
	if capHint >= 64 {
		return make(map[uint64]uint32, min(capHint, batchPostingDeltaMapPoolMaxLen))
	}
	return getFixedBatchPostingAccumMap()
}

func releaseBatchPostingAccumMap(m map[string]uint32) {
	if m == nil {
		return
	}
	oversized := len(m) > batchPostingDeltaMapPoolMaxLen
	clear(m)
	if oversized {
		return
	}
	batchPostingAccumMapPool.Put(m)
}

func releaseFixedBatchPostingAccumMap(m map[uint64]uint32) {
	if m == nil {
		return
	}
	oversized := len(m) > batchPostingDeltaMapPoolMaxLen
	clear(m)
	if oversized {
		return
	}
	fixedBatchPostingAccumMapPool.Put(m)
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
		fieldDelta = getBatchPostingAccumMapHint(capHint)
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
		fieldDelta = getFixedBatchPostingAccumMapHint(capHint)
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

var insertPostingIDsBufPool sync.Pool

func getInsertPostingIDsBuf(capHint int) *insertPostingIDsBuf {
	if v := insertPostingIDsBufPool.Get(); v != nil {
		buf := v.(*insertPostingIDsBuf)
		if cap(buf.values) < capHint {
			buf.values = slices.Grow(buf.values, capHint)
		}
		return buf
	}
	return &insertPostingIDsBuf{values: make([]uint64, 0, max(capHint, insertPostingIDsBufPoolMinCap))}
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
	insertPostingAccumArenaPool sync.Pool
	insertPostingMapPool        sync.Pool
	fixedInsertPostingMapPool   sync.Pool
)

func getInsertPostingAccumArena(capHint int) *insertPostingAccumArena {
	if v := insertPostingAccumArenaPool.Get(); v != nil {
		arena := v.(*insertPostingAccumArena)
		if cap(arena.values) < capHint {
			arena.values = slices.Grow(arena.values, capHint)
		}
		return arena
	}
	return &insertPostingAccumArena{values: make([]insertPostingAccum, 0, max(capHint, 8))}
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

func getInsertPostingMap() map[string]uint32 {
	if v := insertPostingMapPool.Get(); v != nil {
		return v.(map[string]uint32)
	}
	return make(map[string]uint32, 8)
}

func getInsertPostingMapHint(capHint int) map[string]uint32 {
	if capHint >= 64 {
		return make(map[string]uint32, min(capHint, postingMapPoolMaxLen))
	}
	return getInsertPostingMap()
}

func getFixedInsertPostingMap() map[uint64]uint32 {
	if v := fixedInsertPostingMapPool.Get(); v != nil {
		return v.(map[uint64]uint32)
	}
	return make(map[uint64]uint32, 8)
}

func getFixedInsertPostingMapHint(capHint int) map[uint64]uint32 {
	if capHint >= 64 {
		return make(map[uint64]uint32, min(capHint, postingMapPoolMaxLen))
	}
	return getFixedInsertPostingMap()
}

func releaseInsertPostingMap(m map[string]uint32) {
	if m == nil {
		return
	}
	oversized := len(m) > postingMapPoolMaxLen
	clear(m)
	if oversized {
		return
	}
	insertPostingMapPool.Put(m)
}

func releaseFixedInsertPostingMap(m map[uint64]uint32) {
	if m == nil {
		return
	}
	oversized := len(m) > postingMapPoolMaxLen
	clear(m)
	if oversized {
		return
	}
	fixedInsertPostingMapPool.Put(m)
}

func addInsertPostingAccum(
	fieldMap map[string]uint32,
	arena **insertPostingAccumArena,
	key string,
	idx uint64,
	capHint int,
) map[string]uint32 {
	if fieldMap == nil {
		fieldMap = getInsertPostingMapHint(capHint)
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
		fieldMap = getFixedInsertPostingMapHint(capHint)
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
