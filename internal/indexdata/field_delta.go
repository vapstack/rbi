package indexdata

import (
	"slices"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const LenIndexNonEmptyKey = "\xFFNONEMPTY"

type BatchPostingDelta struct {
	Add    posting.List
	Remove posting.List
}

type PostingDelta struct {
	Key   keycodec.IndexKey
	Delta BatchPostingDelta
}

type postingDiffAccum struct {
	add    postingAddAccum
	remove postingAddAccum
}

type PostingDiffArena struct{ values []postingDiffAccum }

type postingAddAccum struct {
	spill     []uint64
	inlineLen uint8
	inline    [posting.SmallCap]uint64
}

type PostingAddArena struct{ values []postingAddAccum }

type LenPostingDiff struct {
	lengths     map[uint64]BatchPostingDelta
	nonEmpty    BatchPostingDelta
	hasNonEmpty bool
}

func getPostingDiffArena(capHint int) *PostingDiffArena {
	arena := postingDiffArenaPool.Get()
	if cap(arena.values) < capHint {
		arena.values = slices.Grow(arena.values, capHint)
	}
	return arena
}

func (arena *PostingDiffArena) Release() {
	if arena == nil {
		return
	}
	arena.Reset()
	postingDiffArenaPool.Put(arena)
}

func (arena *PostingDiffArena) Reset() {
	if arena == nil {
		return
	}
	for i := range arena.values {
		arena.values[i].reset()
	}
	if cap(arena.values) > postingAccumMapMaxLen {
		arena.values = nil
	} else {
		arena.values = arena.values[:0]
	}
}

func (arena *PostingDiffArena) alloc() uint32 {
	ref := uint32(len(arena.values))
	arena.values = append(arena.values, postingDiffAccum{})
	return ref
}

func (arena *PostingDiffArena) accum(ref uint32) *postingDiffAccum {
	return &arena.values[ref]
}

func (acc *postingDiffAccum) reset() {
	acc.add.reset()
	acc.remove.reset()
}

func (acc *postingDiffAccum) addID(id uint64, isAdd bool) {
	if isAdd {
		acc.add.add(id)
		return
	}
	acc.remove.add(id)
}

func (acc *postingDiffAccum) materializeOwned() BatchPostingDelta {
	return BatchPostingDelta{
		Add:    acc.add.materializeOwned(),
		Remove: acc.remove.materializeOwned(),
	}
}

func AddStringPostingDiff(
	fieldDelta map[string]uint32,
	arena **PostingDiffArena,
	key string,
	idx uint64,
	isAdd bool,
	capHint int,
) map[string]uint32 {
	if fieldDelta == nil {
		fieldDelta = stringPostingDiffMapPool.Get(min(capHint, postingAccumMapMaxLen))
	}
	return addStringPostingDiff(fieldDelta, arena, key, idx, isAdd, capHint, true)
}

func AddStringPostingDiffOwned(
	fieldDelta map[string]uint32,
	arena **PostingDiffArena,
	key string,
	idx uint64,
	isAdd bool,
) map[string]uint32 {
	if fieldDelta == nil {
		fieldDelta = make(map[string]uint32, 8)
	}
	return addStringPostingDiff(fieldDelta, arena, key, idx, isAdd, 0, false)
}

func addStringPostingDiff(
	fieldDelta map[string]uint32,
	arena **PostingDiffArena,
	key string,
	idx uint64,
	isAdd bool,
	capHint int,
	pooledArena bool,
) map[string]uint32 {
	ref, ok := fieldDelta[key]
	if !ok {
		if *arena == nil {
			if pooledArena {
				*arena = getPostingDiffArena(capHint)
			} else {
				*arena = &PostingDiffArena{}
			}
		}
		ref = (*arena).alloc()
		fieldDelta[key] = ref
	}
	(*arena).accum(ref).addID(idx, isAdd)
	return fieldDelta
}

func AddFixedPostingDiff(
	fieldDelta map[uint64]uint32,
	arena **PostingDiffArena,
	key uint64,
	idx uint64,
	isAdd bool,
	capHint int,
) map[uint64]uint32 {
	if fieldDelta == nil {
		fieldDelta = fixedPostingDiffMapPool.Get(min(capHint, postingAccumMapMaxLen))
	}
	return addFixedPostingDiff(fieldDelta, arena, key, idx, isAdd, capHint, true)
}

func AddFixedPostingDiffOwned(
	fieldDelta map[uint64]uint32,
	arena **PostingDiffArena,
	key uint64,
	idx uint64,
	isAdd bool,
) map[uint64]uint32 {
	if fieldDelta == nil {
		fieldDelta = make(map[uint64]uint32, 8)
	}
	return addFixedPostingDiff(fieldDelta, arena, key, idx, isAdd, 0, false)
}

func addFixedPostingDiff(
	fieldDelta map[uint64]uint32,
	arena **PostingDiffArena,
	key uint64,
	idx uint64,
	isAdd bool,
	capHint int,
	pooledArena bool,
) map[uint64]uint32 {
	ref, ok := fieldDelta[key]
	if !ok {
		if *arena == nil {
			if pooledArena {
				*arena = getPostingDiffArena(capHint)
			} else {
				*arena = &PostingDiffArena{}
			}
		}
		ref = (*arena).alloc()
		fieldDelta[key] = ref
	}
	(*arena).accum(ref).addID(idx, isAdd)
	return fieldDelta
}

func getPostingAddArena(capHint int) *PostingAddArena {
	arena := postingAddArenaPool.Get()
	if cap(arena.values) < capHint {
		arena.values = slices.Grow(arena.values, capHint)
	}
	return arena
}

func (arena *PostingAddArena) Release() {
	if arena == nil {
		return
	}
	arena.Reset()
	postingAddArenaPool.Put(arena)
}

func (arena *PostingAddArena) Reset() {
	if arena == nil {
		return
	}
	for i := range arena.values {
		arena.values[i].reset()
	}
	if cap(arena.values) > postingAccumMapMaxLen {
		arena.values = nil
	} else {
		arena.values = arena.values[:0]
	}
}

func (arena *PostingAddArena) alloc() uint32 {
	ref := uint32(len(arena.values))
	arena.values = append(arena.values, postingAddAccum{})
	return ref
}

func (arena *PostingAddArena) accum(ref uint32) *postingAddAccum {
	return &arena.values[ref]
}

func (acc *postingAddAccum) reset() {
	pooled.ReleaseUint64Slice(acc.spill)
	acc.spill = nil
	acc.inlineLen = 0
}

func (acc *postingAddAccum) add(id uint64) {
	if acc.spill != nil {
		acc.spill = append(acc.spill, id)
		return
	}
	n := int(acc.inlineLen)
	if n < len(acc.inline) {
		acc.inline[n] = id
		acc.inlineLen++
		return
	}

	spill := pooled.GetUint64Slice(posting.MidCap)
	spill = append(spill, acc.inline[:]...)
	spill = append(spill, id)
	acc.spill = spill
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

func materializePostingAddOwned(ids []uint64) posting.List {
	if len(ids) == 0 {
		return posting.List{}
	}
	if len(ids) > 1 {
		slices.Sort(ids)
		ids = compactSortedUint64s(ids)
	}
	return posting.BuildFromSorted(ids)
}

func (acc *postingAddAccum) materializeOwned() posting.List {
	if acc.spill != nil {
		out := materializePostingAddOwned(acc.spill)
		pooled.ReleaseUint64Slice(acc.spill)
		acc.spill = nil
		return out
	}
	n := int(acc.inlineLen)
	acc.inlineLen = 0
	return materializePostingAddOwned(acc.inline[:n])
}

func AddStringPostingAdd(
	fieldMap map[string]uint32,
	arena **PostingAddArena,
	key string,
	idx uint64,
	capHint int,
) map[string]uint32 {
	if fieldMap == nil {
		fieldMap = stringPostingAddMapPool.Get(min(capHint, postingAccumMapMaxLen))
	}
	return addStringPostingAdd(fieldMap, arena, key, idx, capHint, true)
}

func AddStringPostingAddOwned(
	fieldMap map[string]uint32,
	arena **PostingAddArena,
	key string,
	idx uint64,
	capHint int,
) map[string]uint32 {
	if fieldMap == nil {
		fieldMap = make(map[string]uint32, max(8, min(capHint, postingAccumMapMaxLen)))
	}
	return addStringPostingAdd(fieldMap, arena, key, idx, capHint, false)
}

func addStringPostingAdd(
	fieldMap map[string]uint32,
	arena **PostingAddArena,
	key string,
	idx uint64,
	capHint int,
	pooledArena bool,
) map[string]uint32 {
	ref, ok := fieldMap[key]
	if !ok {
		if *arena == nil {
			if pooledArena {
				*arena = getPostingAddArena(capHint)
			} else {
				*arena = &PostingAddArena{}
			}
		}
		ref = (*arena).alloc()
		fieldMap[key] = ref
	}
	(*arena).accum(ref).add(idx)
	return fieldMap
}

func AddFixedPostingAdd(
	fieldMap map[uint64]uint32,
	arena **PostingAddArena,
	key uint64,
	idx uint64,
	capHint int,
) map[uint64]uint32 {
	if fieldMap == nil {
		fieldMap = fixedPostingAddMapPool.Get(min(capHint, postingAccumMapMaxLen))
	}
	return addFixedPostingAdd(fieldMap, arena, key, idx, capHint, true)
}

func AddFixedPostingAddOwned(
	fieldMap map[uint64]uint32,
	arena **PostingAddArena,
	key uint64,
	idx uint64,
	capHint int,
) map[uint64]uint32 {
	if fieldMap == nil {
		fieldMap = make(map[uint64]uint32, max(8, min(capHint, postingAccumMapMaxLen)))
	}
	return addFixedPostingAdd(fieldMap, arena, key, idx, capHint, false)
}

func addFixedPostingAdd(
	fieldMap map[uint64]uint32,
	arena **PostingAddArena,
	key uint64,
	idx uint64,
	capHint int,
	pooledArena bool,
) map[uint64]uint32 {
	ref, ok := fieldMap[key]
	if !ok {
		if *arena == nil {
			if pooledArena {
				*arena = getPostingAddArena(capHint)
			} else {
				*arena = &PostingAddArena{}
			}
		}
		ref = (*arena).alloc()
		fieldMap[key] = ref
	}
	(*arena).accum(ref).add(idx)
	return fieldMap
}

func (d *LenPostingDiff) putAfterMove() {
	if d == nil {
		return
	}
	if d.lengths != nil {
		ReleaseBatchPostingDeltaMap(d.lengths)
		d.lengths = nil
	}
	d.nonEmpty = BatchPostingDelta{}
	d.hasNonEmpty = false
	lenPostingDiffPool.Put(d)
}

func (d *LenPostingDiff) Release() {
	if d == nil {
		return
	}
	if d.lengths != nil {
		for _, lengthDelta := range d.lengths {
			lengthDelta.Add.Release()
			lengthDelta.Remove.Release()
		}
		ReleaseBatchPostingDeltaMap(d.lengths)
		d.lengths = nil
	}
	d.nonEmpty.Add.Release()
	d.nonEmpty.Remove.Release()
	d.nonEmpty = BatchPostingDelta{}
	d.hasNonEmpty = false
	lenPostingDiffPool.Put(d)
}

func (d *LenPostingDiff) Reset() {
	if d == nil {
		return
	}
	if d.lengths != nil {
		for _, lengthDelta := range d.lengths {
			lengthDelta.Add.Release()
			lengthDelta.Remove.Release()
		}
		if len(d.lengths) > 4<<10 {
			d.lengths = nil
		} else {
			clear(d.lengths)
		}
	}
	d.nonEmpty.Add.Release()
	d.nonEmpty.Remove.Release()
	d.nonEmpty = BatchPostingDelta{}
	d.hasNonEmpty = false
}

func (d *LenPostingDiff) resetAfterMove() {
	if d == nil {
		return
	}
	if d.lengths != nil {
		if len(d.lengths) > 4<<10 {
			d.lengths = nil
		} else {
			clear(d.lengths)
		}
	}
	d.nonEmpty = BatchPostingDelta{}
	d.hasNonEmpty = false
}

func ensureLenPostingDiff(delta **LenPostingDiff) *LenPostingDiff {
	if *delta == nil {
		*delta = lenPostingDiffPool.Get()
		if (*delta).lengths == nil {
			(*delta).lengths = GetBatchPostingDeltaMap()
		}
	}
	return *delta
}

func AddLenPostingBucketDiff(fieldDelta **LenPostingDiff, idx uint64, length int, isAdd bool) {
	delta := ensureLenPostingDiff(fieldDelta)
	delta.lengths = addFixedBatchPostingDelta(delta.lengths, uint64(length), idx, isAdd)
}

func AddLenPostingNonEmptyDiff(fieldDelta **LenPostingDiff, idx uint64, isAdd bool) {
	delta := ensureLenPostingDiff(fieldDelta)
	delta.AddNonEmpty(idx, isAdd)
}

func AddLenPostingBucket(fieldDelta *LenPostingDiff, idx uint64, length int, isAdd bool) *LenPostingDiff {
	if fieldDelta == nil {
		fieldDelta = &LenPostingDiff{}
	}
	fieldDelta.AddBucket(idx, length, isAdd)
	return fieldDelta
}

func AddLenPostingNonEmpty(fieldDelta *LenPostingDiff, idx uint64, isAdd bool) *LenPostingDiff {
	if fieldDelta == nil {
		fieldDelta = &LenPostingDiff{}
	}
	fieldDelta.AddNonEmpty(idx, isAdd)
	return fieldDelta
}

func (d *LenPostingDiff) AddBucket(idx uint64, length int, isAdd bool) {
	if d.lengths == nil {
		d.lengths = make(map[uint64]BatchPostingDelta, 64)
	}
	delta := d.lengths[uint64(length)]
	if isAdd {
		delta.Add = delta.Add.BuildAdded(idx)
	} else {
		delta.Remove = delta.Remove.BuildAdded(idx)
	}
	d.lengths[uint64(length)] = delta
}

func (d *LenPostingDiff) AddNonEmpty(idx uint64, isAdd bool) {
	d.hasNonEmpty = true
	if isAdd {
		d.nonEmpty.Add = d.nonEmpty.Add.BuildAdded(idx)
	} else {
		d.nonEmpty.Remove = d.nonEmpty.Remove.BuildAdded(idx)
	}
}

func addFixedBatchPostingDelta(fieldDelta map[uint64]BatchPostingDelta, key uint64, idx uint64, isAdd bool) map[uint64]BatchPostingDelta {
	if fieldDelta == nil {
		fieldDelta = GetBatchPostingDeltaMap()
	}
	delta := fieldDelta[key]
	if isAdd {
		delta.Add = delta.Add.BuildAdded(idx)
	} else {
		delta.Remove = delta.Remove.BuildAdded(idx)
	}
	fieldDelta[key] = delta
	return fieldDelta
}

func sortedStringPostingDeltasBufOwned(deltas map[string]uint32, arena *PostingDiffArena, fixed8 bool) []PostingDelta {
	if len(deltas) == 0 {
		return nil
	}

	keys := pooled.GetStringSlice(len(deltas))
	for raw := range deltas {
		keys = append(keys, raw)
	}
	slices.Sort(keys)

	buf := GetPostingDeltaSlice(len(deltas))
	for _, raw := range keys {
		ref := deltas[raw]
		delta := arena.accum(ref).materializeOwned()
		if delta.Add.IsEmpty() && delta.Remove.IsEmpty() {
			continue
		}
		buf = append(buf, PostingDelta{
			Key:   keycodec.FromStoredString(raw, fixed8),
			Delta: delta,
		})
	}
	pooled.ReleaseStringSlice(keys)
	if len(buf) == 0 {
		ReleasePostingDeltaSlice(buf)
		return nil
	}
	return buf
}

func sortedFixedPostingDeltasBufOwned(deltas map[uint64]uint32, arena *PostingDiffArena) []PostingDelta {
	if len(deltas) == 0 {
		return nil
	}

	keys := pooled.GetUint64Slice(len(deltas))
	for raw := range deltas {
		keys = append(keys, raw)
	}
	slices.Sort(keys)

	buf := GetPostingDeltaSlice(len(deltas))
	for _, raw := range keys {
		ref := deltas[raw]
		delta := arena.accum(ref).materializeOwned()
		if delta.Add.IsEmpty() && delta.Remove.IsEmpty() {
			continue
		}
		buf = append(buf, PostingDelta{
			Key:   keycodec.FromU64(raw),
			Delta: delta,
		})
	}
	pooled.ReleaseUint64Slice(keys)
	if len(buf) == 0 {
		ReleasePostingDeltaSlice(buf)
		return nil
	}
	return buf
}

func (s FieldStorage) ApplyStringPostingDiffOwned(
	deltas map[string]uint32,
	arena *PostingDiffArena,
	fixed8 bool,
	allowChunk bool,
) FieldStorage {
	defer PutStringPostingDiffMap(deltas)
	return s.ApplyStringPostingDiffRetainMapOwned(deltas, arena, fixed8, allowChunk)
}

func (s FieldStorage) ApplyStringPostingDiffRetainMapOwned(
	deltas map[string]uint32,
	arena *PostingDiffArena,
	fixed8 bool,
	allowChunk bool,
) FieldStorage {
	buf := sortedStringPostingDeltasBufOwned(deltas, arena, fixed8)
	if buf == nil {
		return s
	}
	return s.applyPostingDiffBufOwned(buf, allowChunk)
}

func (s FieldStorage) ApplyFixedPostingDiffOwned(
	deltas map[uint64]uint32,
	arena *PostingDiffArena,
	allowChunk bool,
) FieldStorage {
	defer PutFixedPostingDiffMap(deltas)
	return s.ApplyFixedPostingDiffRetainMapOwned(deltas, arena, allowChunk)
}

func (s FieldStorage) ApplyFixedPostingDiffRetainMapOwned(
	deltas map[uint64]uint32,
	arena *PostingDiffArena,
	allowChunk bool,
) FieldStorage {
	buf := sortedFixedPostingDeltasBufOwned(deltas, arena)
	if buf == nil {
		return s
	}
	return s.applyPostingDiffBufOwned(buf, allowChunk)
}

func sortedStringPostingAddsBufOwned(adds map[string]uint32, arena *PostingAddArena, fixed8 bool) []PostingDelta {
	if len(adds) == 0 {
		return nil
	}
	keys := pooled.GetStringSlice(len(adds))
	for raw := range adds {
		keys = append(keys, raw)
	}
	slices.Sort(keys)

	buf := GetPostingDeltaSlice(len(adds))
	for _, raw := range keys {
		ref := adds[raw]
		ids := arena.accum(ref).materializeOwned()
		buf = append(buf, PostingDelta{
			Key: keycodec.FromStoredString(raw, fixed8),
			Delta: BatchPostingDelta{
				Add: ids,
			},
		})
	}
	pooled.ReleaseStringSlice(keys)
	return buf
}

func sortedFixedPostingAddsBufOwned(adds map[uint64]uint32, arena *PostingAddArena) []PostingDelta {
	if len(adds) == 0 {
		return nil
	}
	keys := pooled.GetUint64Slice(len(adds))
	for raw := range adds {
		keys = append(keys, raw)
	}
	slices.Sort(keys)

	buf := GetPostingDeltaSlice(len(adds))
	for _, raw := range keys {
		ref := adds[raw]
		ids := arena.accum(ref).materializeOwned()
		buf = append(buf, PostingDelta{
			Key: keycodec.FromU64(raw),
			Delta: BatchPostingDelta{
				Add: ids,
			},
		})
	}
	pooled.ReleaseUint64Slice(keys)
	return buf
}

func (s FieldStorage) MergeStringPostingAddsOwned(
	adds map[string]uint32,
	arena *PostingAddArena,
	fixed8 bool,
	allowChunk bool,
) FieldStorage {
	defer PutStringPostingAddMap(adds)
	return s.MergeStringPostingAddsRetainMapOwned(adds, arena, fixed8, allowChunk)
}

func (s FieldStorage) MergeStringPostingAddsRetainMapOwned(
	adds map[string]uint32,
	arena *PostingAddArena,
	fixed8 bool,
	allowChunk bool,
) FieldStorage {
	if len(adds) == 0 {
		return s
	}
	buf := sortedStringPostingAddsBufOwned(adds, arena, fixed8)
	if buf == nil {
		return s
	}
	return s.applyPostingDiffBufOwned(buf, allowChunk)
}

func (s FieldStorage) MergeFixedPostingAddsOwned(
	adds map[uint64]uint32,
	arena *PostingAddArena,
	allowChunk bool,
) FieldStorage {
	defer PutFixedPostingAddMap(adds)
	return s.MergeFixedPostingAddsRetainMapOwned(adds, arena, allowChunk)
}

func (s FieldStorage) MergeFixedPostingAddsRetainMapOwned(
	adds map[uint64]uint32,
	arena *PostingAddArena,
	allowChunk bool,
) FieldStorage {
	if len(adds) == 0 {
		return s
	}
	buf := sortedFixedPostingAddsBufOwned(adds, arena)
	if buf == nil {
		return s
	}
	return s.applyPostingDiffBufOwned(buf, allowChunk)
}

func (d *LenPostingDiff) sortedBufOwned() []PostingDelta {
	if d == nil {
		return nil
	}
	count := len(d.lengths)
	if d.hasNonEmpty {
		count++
	}
	if count == 0 {
		return nil
	}

	buf := GetPostingDeltaSlice(count)
	for raw, delta := range d.lengths {
		if delta.Add.IsEmpty() && delta.Remove.IsEmpty() {
			continue
		}
		buf = append(buf, PostingDelta{
			Key:   keycodec.FromU64(raw),
			Delta: delta,
		})
	}
	if d.hasNonEmpty && (!d.nonEmpty.Add.IsEmpty() || !d.nonEmpty.Remove.IsEmpty()) {
		buf = append(buf, PostingDelta{
			Key:   keycodec.FromString(LenIndexNonEmptyKey),
			Delta: d.nonEmpty,
		})
	}
	if len(buf) == 0 {
		ReleasePostingDeltaSlice(buf)
		return nil
	}
	sortPostingDeltasBuf(buf)
	return buf
}

func (d *LenPostingDiff) count() int {
	if d == nil {
		return 0
	}
	count := 0
	for _, delta := range d.lengths {
		if delta.Add.IsEmpty() && delta.Remove.IsEmpty() {
			continue
		}
		count++
	}
	if d.hasNonEmpty && (!d.nonEmpty.Add.IsEmpty() || !d.nonEmpty.Remove.IsEmpty()) {
		count++
	}
	return count
}

func (d *LenPostingDiff) takeOwned(dst []PostingDelta) int {
	if d == nil || len(dst) == 0 {
		return 0
	}
	n := 0
	for raw, delta := range d.lengths {
		if delta.Add.IsEmpty() && delta.Remove.IsEmpty() {
			continue
		}
		dst[n] = PostingDelta{
			Key:   keycodec.FromU64(raw),
			Delta: delta,
		}
		n++
		if n == len(dst) {
			break
		}
	}
	if n < len(dst) && d.hasNonEmpty && (!d.nonEmpty.Add.IsEmpty() || !d.nonEmpty.Remove.IsEmpty()) {
		dst[n] = PostingDelta{
			Key:   keycodec.FromString(LenIndexNonEmptyKey),
			Delta: d.nonEmpty,
		}
		n++
	}
	return n
}

func (s FieldStorage) ApplyLenPostingDiffOwned(deltas *LenPostingDiff) FieldStorage {
	return s.applyLenPostingDiffOwned(deltas, false)
}

func (s FieldStorage) ApplyLenPostingDiffRetainOwned(deltas *LenPostingDiff) FieldStorage {
	return s.applyLenPostingDiffOwned(deltas, true)
}

func (s FieldStorage) applyLenPostingDiffOwned(deltas *LenPostingDiff, retain bool) FieldStorage {
	count := deltas.count()
	if count == 0 {
		deltas.finishAfterMove(retain)
		return s
	}
	if count == 1 {
		delta, ok := deltas.takeSingleOwned()
		deltas.finishAfterMove(retain)
		if !ok {
			return s
		}
		storage := s.applyPostingDiff([]PostingDelta{delta}, s.IsChunked())
		if storage.KeyCount() > 0 {
			return storage
		}
		return FieldStorage{}
	}
	var deltaKeys []PostingDelta
	var inline [2]PostingDelta
	var buf []PostingDelta
	if count <= len(inline) {
		n := deltas.takeOwned(inline[:count])
		deltas.finishAfterMove(retain)
		deltaKeys = inline[:n]
		if len(deltaKeys) > 1 && keycodec.Compare(deltaKeys[0].Key, deltaKeys[1].Key) > 0 {
			deltaKeys[0], deltaKeys[1] = deltaKeys[1], deltaKeys[0]
		}
	} else {
		buf = deltas.sortedBufOwned()
		deltas.finishAfterMove(retain)
		if buf == nil {
			return s
		}
	}

	var storage FieldStorage
	if buf != nil {
		storage = s.applyPostingDiffBufOwned(buf, s.IsChunked())
	} else {
		storage = s.applyPostingDiff(deltaKeys, s.IsChunked())
	}
	if storage.KeyCount() > 0 {
		return storage
	}
	return FieldStorage{}
}

func (d *LenPostingDiff) finishAfterMove(retain bool) {
	if retain {
		d.resetAfterMove()
		return
	}
	d.putAfterMove()
}

func (d *LenPostingDiff) takeSingleOwned() (PostingDelta, bool) {
	if d == nil {
		return PostingDelta{}, false
	}
	for raw, delta := range d.lengths {
		if delta.Add.IsEmpty() && delta.Remove.IsEmpty() {
			continue
		}
		return PostingDelta{
			Key:   keycodec.FromU64(raw),
			Delta: delta,
		}, true
	}
	if d.hasNonEmpty && (!d.nonEmpty.Add.IsEmpty() || !d.nonEmpty.Remove.IsEmpty()) {
		delta := d.nonEmpty
		return PostingDelta{
			Key:   keycodec.FromString(LenIndexNonEmptyKey),
			Delta: delta,
		}, true
	}
	return PostingDelta{}, false
}

func sortPostingDeltasBuf(buf []PostingDelta) {
	n := len(buf)
	if n <= 1 {
		return
	}

	for start := n >> 1; start > 0; {
		start--
		root := start
		for {
			child := root*2 + 1
			if child >= n {
				break
			}
			if right := child + 1; right < n && keycodec.Compare(buf[child].Key, buf[right].Key) < 0 {
				child = right
			}
			if keycodec.Compare(buf[root].Key, buf[child].Key) >= 0 {
				break
			}
			buf[root], buf[child] = buf[child], buf[root]
			root = child
		}
	}

	for end := n; end > 1; {
		end--
		buf[0], buf[end] = buf[end], buf[0]
		root := 0
		for {
			child := root*2 + 1
			if child >= end {
				break
			}
			if right := child + 1; right < end && keycodec.Compare(buf[child].Key, buf[right].Key) < 0 {
				child = right
			}
			if keycodec.Compare(buf[root].Key, buf[child].Key) >= 0 {
				break
			}
			buf[root], buf[child] = buf[child], buf[root]
			root = child
		}
	}
}

func takePostingDeltaBuf(buf []PostingDelta, i int) PostingDelta {
	delta := buf[i]
	buf[i] = PostingDelta{}
	return delta
}

func (r *fieldIndexChunkedRoot) nextPostingDiffEntry(endChunk, chunkIdx, entryIdx int) (Entry, bool) {
	for chunkIdx < endChunk {
		ref, _ := r.refAtChunk(chunkIdx)
		chunk := ref.chunk
		if entryIdx < chunk.keyCount() {
			return Entry{Key: chunk.keyAt(entryIdx), IDs: chunk.postingAt(entryIdx)}, true
		}
		chunkIdx++
		entryIdx = 0
	}
	return Entry{}, false
}

func (r *fieldIndexChunkedRoot) advancePostingDiffEntry(endChunk int, chunkIdx *int, entryIdx *int) {
	*entryIdx += 1
	for *chunkIdx < endChunk {
		ref, _ := r.refAtChunk(*chunkIdx)
		if *entryIdx < ref.chunk.keyCount() {
			return
		}
		*chunkIdx += 1
		*entryIdx = 0
	}
}

func (d BatchPostingDelta) mutationCardinality() uint64 {
	return d.Add.Cardinality() + d.Remove.Cardinality()
}

func postingListLooksRunFriendly(ids posting.List) bool {
	if ids.IsEmpty() {
		return false
	}
	if _, ok := ids.TrySingle(); ok {
		return false
	}
	card := ids.Cardinality()
	if card < 2 {
		return false
	}
	minID, ok := ids.Minimum()
	if !ok {
		return false
	}
	maxID, ok := ids.Maximum()
	if !ok || maxID < minID {
		return false
	}
	span := maxID - minID + 1
	return span <= card*2
}

func (d BatchPostingDelta) shouldOptimize(base posting.List, out posting.List) bool {
	if out.IsEmpty() {
		return false
	}
	if _, ok := out.TrySingle(); ok {
		return false
	}
	if postingListLooksRunFriendly(d.Add) || postingListLooksRunFriendly(d.Remove) || postingListLooksRunFriendly(out) {
		return true
	}
	if base.IsEmpty() {
		return false
	}
	if _, ok := base.TrySingle(); ok {
		return false
	}
	mutation := d.mutationCardinality()
	if mutation == 0 {
		return false
	}
	baseCard := base.Cardinality()
	return baseCard > 0 && mutation*4 >= baseCard
}

func (d *BatchPostingDelta) applyOwned(base posting.List) posting.List {
	deltaValue := *d
	out := base
	changed := false
	releaseRemove := !deltaValue.Remove.IsEmpty()
	releaseAdd := !deltaValue.Add.IsEmpty()

	if !deltaValue.Remove.IsEmpty() && !out.IsEmpty() {
		out = out.Clone()
		out = out.BuildAndNot(deltaValue.Remove)
		changed = true
	}

	if !deltaValue.Add.IsEmpty() {
		if out.IsEmpty() {
			out = deltaValue.Add
			releaseAdd = false
			changed = true
		} else {
			if !changed {
				out = out.Clone()
				changed = true
			}
			out = out.BuildOr(deltaValue.Add)
		}
	}

	if changed && deltaValue.shouldOptimize(base, out) {
		out = out.BuildOptimized()
	}
	if releaseRemove {
		deltaValue.Remove.Release()
	}
	if releaseAdd {
		deltaValue.Add.Release()
	}
	d.Remove = posting.List{}
	d.Add = posting.List{}
	return out
}

func applyFieldPostingDiffSorted(base []Entry, deltaKeys []PostingDelta) ([]Entry, bool) {
	if len(deltaKeys) == 0 {
		return base, true
	}
	if len(deltaKeys) == 1 {
		return applySingleFieldPostingDiffSorted(base, deltaKeys[0])
	}

	src := base
	out := GetFieldEntrySlice(len(src) + len(deltaKeys))

	i, j := 0, 0
	for i < len(src) || j < len(deltaKeys) {
		switch {

		case j >= len(deltaKeys):
			out = appendBorrowedIndexEntries(out, src[i:])
			i = len(src)

		case i >= len(src):
			ids := deltaKeys[j].Delta.applyOwned(posting.List{})
			if !ids.IsEmpty() {
				out = append(out, Entry{Key: deltaKeys[j].Key, IDs: ids})
			}
			j++

		default:
			cmp := keycodec.Compare(src[i].Key, deltaKeys[j].Key)
			switch {

			case cmp < 0:
				out = append(out, src[i].borrow())
				i++

			case cmp > 0:
				ids := deltaKeys[j].Delta.applyOwned(posting.List{})
				if !ids.IsEmpty() {
					out = append(out, Entry{Key: deltaKeys[j].Key, IDs: ids})
				}
				j++

			default:
				ids := deltaKeys[j].Delta.applyOwned(src[i].IDs)
				if !ids.IsEmpty() {
					if ids.SharesPayload(src[i].IDs) {
						out = append(out, src[i].borrow())
					} else {
						out = append(out, Entry{Key: src[i].Key, IDs: ids})
					}
				}
				i++
				j++
			}
		}
	}

	if len(out) == 0 {
		ReleaseFieldEntrySlice(out)
		return nil, false
	}

	return out, false
}

func applyFieldPostingDiffSortedBuf(base []Entry, deltaKeys []PostingDelta) ([]Entry, bool) {
	if deltaKeys == nil || len(deltaKeys) == 0 {
		return base, true
	}
	if len(deltaKeys) == 1 {
		return applySingleFieldPostingDiffSorted(base, takePostingDeltaBuf(deltaKeys, 0))
	}

	src := base
	out := GetFieldEntrySlice(len(src) + len(deltaKeys))

	i, j := 0, 0
	for i < len(src) || j < len(deltaKeys) {
		switch {

		case j >= len(deltaKeys):
			out = appendBorrowedIndexEntries(out, src[i:])
			i = len(src)

		case i >= len(src):
			delta := takePostingDeltaBuf(deltaKeys, j)
			ids := delta.Delta.applyOwned(posting.List{})
			if !ids.IsEmpty() {
				out = append(out, Entry{Key: delta.Key, IDs: ids})
			}
			j++

		default:
			delta := deltaKeys[j]
			cmp := keycodec.Compare(src[i].Key, delta.Key)
			switch {

			case cmp < 0:
				out = append(out, src[i].borrow())
				i++

			case cmp > 0:
				delta = takePostingDeltaBuf(deltaKeys, j)
				ids := delta.Delta.applyOwned(posting.List{})
				if !ids.IsEmpty() {
					out = append(out, Entry{Key: delta.Key, IDs: ids})
				}
				j++

			default:
				delta = takePostingDeltaBuf(deltaKeys, j)
				ids := delta.Delta.applyOwned(src[i].IDs)
				if !ids.IsEmpty() {
					if ids.SharesPayload(src[i].IDs) {
						out = append(out, src[i].borrow())
					} else {
						out = append(out, Entry{Key: src[i].Key, IDs: ids})
					}
				}
				i++
				j++
			}
		}
	}

	if len(out) == 0 {
		ReleaseFieldEntrySlice(out)
		return nil, false
	}
	return out, false
}

func applySingleFieldPostingDiffSorted(base []Entry, delta PostingDelta) ([]Entry, bool) {
	src := base
	if len(src) == 0 {
		ids := delta.Delta.applyOwned(posting.List{})
		if ids.IsEmpty() {
			return nil, false
		}
		out := GetFieldEntrySlice(1)
		out = append(out, Entry{Key: delta.Key, IDs: ids})
		return out, false
	}

	pos := lowerBoundIndexEntriesKey(src, delta.Key)
	if pos >= len(src) || keycodec.Compare(src[pos].Key, delta.Key) > 0 {
		ids := delta.Delta.applyOwned(posting.List{})
		if ids.IsEmpty() {
			return base, true
		}
		out := GetFieldEntrySlice(len(src) + 1)[:len(src)+1]
		copyBorrowedIndexEntries(out[:pos], src[:pos])
		out[pos] = Entry{Key: delta.Key, IDs: ids}
		copyBorrowedIndexEntries(out[pos+1:], src[pos:])
		return out, false
	}

	ids := delta.Delta.applyOwned(src[pos].IDs)
	if ids.SharesPayload(src[pos].IDs) {
		return base, true
	}

	if ids.IsEmpty() {
		if len(src) == 1 {
			return nil, false
		}
		out := GetFieldEntrySlice(len(src) - 1)[:len(src)-1]
		copyBorrowedIndexEntries(out[:pos], src[:pos])
		copyBorrowedIndexEntries(out[pos:], src[pos+1:])
		return out, false
	}

	out := GetFieldEntrySlice(len(src))[:len(src)]
	copyBorrowedIndexEntries(out, src)
	out[pos].IDs = ids
	return out, false
}

func (b *fieldIndexChunkBuilder) appendPostingDiffFlatSorted(src []Entry, deltaKeys []PostingDelta) {
	if b == nil {
		return
	}

	if len(src) == 0 && len(deltaKeys) == 0 {
		return
	}

	numeric := false
	if len(src) > 0 {
		numeric = src[0].Key.IsNumeric()
	} else if len(deltaKeys) > 0 {
		numeric = deltaKeys[0].Key.IsNumeric()
	}
	out := newFieldIndexChunkStreamBuilder(numeric)

	i, j := 0, 0
	for i < len(src) || j < len(deltaKeys) {
		switch {

		case j >= len(deltaKeys):
			out.append(b, src[i].Key, src[i].IDs.Borrow())
			i++

		case i >= len(src):
			ids := deltaKeys[j].Delta.applyOwned(posting.List{})
			if !ids.IsEmpty() {
				out.append(b, deltaKeys[j].Key, ids)
			}
			j++

		default:
			cmp := keycodec.Compare(src[i].Key, deltaKeys[j].Key)
			switch {

			case cmp < 0:
				out.append(b, src[i].Key, src[i].IDs.Borrow())
				i++

			case cmp > 0:
				ids := deltaKeys[j].Delta.applyOwned(posting.List{})
				if !ids.IsEmpty() {
					out.append(b, deltaKeys[j].Key, ids)
				}
				j++

			default:
				ids := deltaKeys[j].Delta.applyOwned(src[i].IDs)
				if !ids.IsEmpty() {
					if ids.SharesPayload(src[i].IDs) {
						out.append(b, src[i].Key, src[i].IDs.Borrow())
					} else {
						out.append(b, src[i].Key, ids)
					}
				}
				i++
				j++
			}
		}
	}

	out.finish(b)
}

func (b *fieldIndexChunkBuilder) appendPostingDiffFlatSortedBuf(src []Entry, deltaKeys []PostingDelta) {
	if b == nil {
		return
	}

	if len(src) == 0 && (deltaKeys == nil || len(deltaKeys) == 0) {
		return
	}

	numeric := false
	if len(src) > 0 {
		numeric = src[0].Key.IsNumeric()
	} else if deltaKeys != nil && len(deltaKeys) > 0 {
		numeric = deltaKeys[0].Key.IsNumeric()
	}
	out := newFieldIndexChunkStreamBuilder(numeric)

	i, j := 0, 0
	for i < len(src) || (deltaKeys != nil && j < len(deltaKeys)) {
		switch {

		case deltaKeys == nil || j >= len(deltaKeys):
			out.append(b, src[i].Key, src[i].IDs.Borrow())
			i++

		case i >= len(src):
			delta := takePostingDeltaBuf(deltaKeys, j)
			ids := delta.Delta.applyOwned(posting.List{})
			if !ids.IsEmpty() {
				out.append(b, delta.Key, ids)
			}
			j++

		default:
			delta := deltaKeys[j]
			cmp := keycodec.Compare(src[i].Key, delta.Key)
			switch {

			case cmp < 0:
				out.append(b, src[i].Key, src[i].IDs.Borrow())
				i++

			case cmp > 0:
				delta = takePostingDeltaBuf(deltaKeys, j)
				ids := delta.Delta.applyOwned(posting.List{})
				if !ids.IsEmpty() {
					out.append(b, delta.Key, ids)
				}
				j++

			default:
				delta = takePostingDeltaBuf(deltaKeys, j)
				ids := delta.Delta.applyOwned(src[i].IDs)
				if !ids.IsEmpty() {
					if ids.SharesPayload(src[i].IDs) {
						out.append(b, src[i].Key, src[i].IDs.Borrow())
					} else {
						out.append(b, src[i].Key, ids)
					}
				}
				i++
				j++
			}
		}
	}

	out.finish(b)
}

func applyFieldPostingDiffFlatMaybeChunked(base []Entry, deltaKeys []PostingDelta) FieldStorage {
	est := len(deltaKeys) + len(base)
	builder := newFieldIndexChunkBuilder(est)
	builder.appendPostingDiffFlatSorted(base, deltaKeys)

	root := builder.root()
	if root == nil {
		return FieldStorage{}
	}

	if root.keyCount < fieldIndexChunkThreshold {
		entries, data := root.flatten()
		flat := newFlatFieldStorage(entries, data)
		root.release()
		return flat
	}

	return newChunkedFieldStorage(root)
}

func applyFieldPostingDiffFlatMaybeChunkedBuf(base []Entry, deltaKeys []PostingDelta) FieldStorage {
	est := 0
	if deltaKeys != nil {
		est = len(deltaKeys)
	}
	est += len(base)

	builder := newFieldIndexChunkBuilder(est)
	builder.appendPostingDiffFlatSortedBuf(base, deltaKeys)

	root := builder.root()
	if root == nil {
		return FieldStorage{}
	}

	if root.keyCount < fieldIndexChunkThreshold {
		entries, data := root.flatten()
		flat := newFlatFieldStorage(entries, data)
		root.release()
		return flat
	}

	return newChunkedFieldStorage(root)
}

func (s FieldStorage) applyPostingDiff(deltaKeys []PostingDelta, allowChunk bool) FieldStorage {
	if len(deltaKeys) == 0 {
		return s
	}

	if !allowChunk {
		var flat []Entry
		if s.flat != nil {
			flat = s.flat.entries
		}
		updated, same := applyFieldPostingDiffSorted(flat, deltaKeys)
		if same {
			return s
		}
		return newFlatFieldStorage(updated, nil)
	}

	if s.chunked != nil {
		return s.chunked.applyPostingDiff(deltaKeys)
	}

	var flat []Entry
	if s.flat != nil {
		flat = s.flat.entries
	}

	if len(deltaKeys)+len(flat) >= fieldIndexChunkThreshold {
		return applyFieldPostingDiffFlatMaybeChunked(flat, deltaKeys)
	}

	updated, same := applyFieldPostingDiffSorted(flat, deltaKeys)
	if same {
		return s
	}

	return newRegularFieldStorage(updated)
}

func (s FieldStorage) applyPostingDiffBufOwned(buf []PostingDelta, allowChunk bool) FieldStorage {
	if buf == nil {
		return s
	}
	defer ReleasePostingDeltaSlice(buf)

	if len(buf) == 0 {
		return s
	}

	if !allowChunk {
		var flat []Entry
		if s.flat != nil {
			flat = s.flat.entries
		}
		updated, same := applyFieldPostingDiffSortedBuf(flat, buf)
		if same {
			return s
		}
		return newFlatFieldStorage(updated, nil)
	}

	if s.chunked != nil {
		return s.chunked.applyPostingDiffBuf(buf)
	}

	var flat []Entry
	if s.flat != nil {
		flat = s.flat.entries
	}

	if len(buf)+len(flat) >= fieldIndexChunkThreshold {
		return applyFieldPostingDiffFlatMaybeChunkedBuf(flat, buf)
	}

	updated, same := applyFieldPostingDiffSortedBuf(flat, buf)
	if same {
		return s
	}

	return newRegularFieldStorage(updated)
}

func (b *fieldIndexChunkBuilder) appendPostingDiffChunkRangeSorted(
	base *fieldIndexChunkedRoot,
	startChunk, endChunk int,
	deltaKeys []PostingDelta,
) {
	if b == nil || base == nil || startChunk < 0 || endChunk < startChunk || endChunk > base.chunkCount {
		return
	}

	if len(deltaKeys) == 0 {
		b.appendRefsRange(base, startChunk, endChunk)
		return
	}

	numeric := false
	if startChunk < endChunk {
		if ref, ok := base.refAtChunk(startChunk); ok && ref.chunk != nil && ref.chunk.hasNumericKeys() {
			numeric = true
		}
	} else if len(deltaKeys) > 0 {
		numeric = deltaKeys[0].Key.IsNumeric()
	}

	out := newFieldIndexChunkStreamBuilder(numeric)

	chunkIdx := startChunk
	entryIdx := 0
	j := 0

	for {
		baseEnt, hasBase := base.nextPostingDiffEntry(endChunk, chunkIdx, entryIdx)
		switch {

		case !hasBase && j >= len(deltaKeys):
			out.finish(b)
			return

		case !hasBase:
			ids := deltaKeys[j].Delta.applyOwned(posting.List{})
			if !ids.IsEmpty() {
				out.append(b, deltaKeys[j].Key, ids)
			}
			j++

		case j >= len(deltaKeys):
			out.append(b, baseEnt.Key, baseEnt.IDs)
			base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)

		default:
			cmp := keycodec.Compare(baseEnt.Key, deltaKeys[j].Key)
			switch {

			case cmp < 0:
				out.append(b, baseEnt.Key, baseEnt.IDs)
				base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)

			case cmp > 0:
				ids := deltaKeys[j].Delta.applyOwned(posting.List{})
				if !ids.IsEmpty() {
					out.append(b, deltaKeys[j].Key, ids)
				}
				j++

			default:
				ids := deltaKeys[j].Delta.applyOwned(baseEnt.IDs)
				if !ids.IsEmpty() {
					out.append(b, baseEnt.Key, ids)
				}
				base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)
				j++
			}
		}
	}
}

func (b *fieldIndexChunkBuilder) appendPostingDiffChunkRangeSortedBuf(
	base *fieldIndexChunkedRoot,
	startChunk, endChunk int,
	deltaKeys []PostingDelta,
	deltaStart, deltaEnd int,
) {
	if b == nil || base == nil || startChunk < 0 || endChunk < startChunk || endChunk > base.chunkCount {
		return
	}

	if deltaKeys == nil || deltaStart >= deltaEnd {
		b.appendRefsRange(base, startChunk, endChunk)
		return
	}

	numeric := false
	if startChunk < endChunk {
		if ref, ok := base.refAtChunk(startChunk); ok && ref.chunk != nil && ref.chunk.hasNumericKeys() {
			numeric = true
		}
	} else {
		numeric = deltaKeys[deltaStart].Key.IsNumeric()
	}

	out := newFieldIndexChunkStreamBuilder(numeric)

	chunkIdx := startChunk
	entryIdx := 0
	j := deltaStart

	for {
		baseEnt, hasBase := base.nextPostingDiffEntry(endChunk, chunkIdx, entryIdx)

		switch {
		case !hasBase && j >= deltaEnd:
			out.finish(b)
			return

		case !hasBase:
			delta := takePostingDeltaBuf(deltaKeys, j)
			ids := delta.Delta.applyOwned(posting.List{})
			if !ids.IsEmpty() {
				out.append(b, delta.Key, ids)
			}
			j++

		case j >= deltaEnd:
			out.append(b, baseEnt.Key, baseEnt.IDs)
			base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)

		default:
			delta := deltaKeys[j]
			cmp := keycodec.Compare(baseEnt.Key, delta.Key)

			switch {
			case cmp < 0:
				out.append(b, baseEnt.Key, baseEnt.IDs)
				base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)

			case cmp > 0:
				delta = takePostingDeltaBuf(deltaKeys, j)
				ids := delta.Delta.applyOwned(posting.List{})
				if !ids.IsEmpty() {
					out.append(b, delta.Key, ids)
				}
				j++

			default:
				delta = takePostingDeltaBuf(deltaKeys, j)
				ids := delta.Delta.applyOwned(baseEnt.IDs)
				if !ids.IsEmpty() {
					out.append(b, baseEnt.Key, ids)
				}
				base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)
				j++
			}
		}
	}
}

func (r *fieldIndexChunkedRoot) applyPostingDiff(deltaKeys []PostingDelta) FieldStorage {
	if r == nil || r.keyCount == 0 {
		return applyFieldPostingDiffFlatMaybeChunked(nil, deltaKeys)
	}
	if len(deltaKeys) == 0 {
		return newChunkedFieldStorage(r)
	}
	if len(deltaKeys) == 1 {
		return r.applySinglePostingDiff(deltaKeys[0])
	}

	builder := newFieldIndexChunkBuilder(r.keyCount + len(deltaKeys))
	chunkIdx := 0
	deltaPos := 0
	touchIdx := 0

	for deltaPos < len(deltaKeys) {
		touchIdx = r.touchChunkIndexFrom(max(touchIdx, chunkIdx), deltaKeys[deltaPos].Key)
		if touchIdx < 0 {
			break
		}
		if chunkIdx < touchIdx {
			builder.appendRefsRange(r, chunkIdx, touchIdx)
		}

		runStart := touchIdx
		runEnd := touchIdx + 1
		deltaStart := deltaPos
		deltaPos++
		for deltaPos < len(deltaKeys) {
			touchIdx = r.touchChunkIndexFrom(touchIdx, deltaKeys[deltaPos].Key)
			if touchIdx > runEnd {
				break
			}
			if touchIdx == runEnd {
				runEnd++
			}
			deltaPos++
		}

		builder.appendPostingDiffChunkRangeSorted(r, runStart, runEnd, deltaKeys[deltaStart:deltaPos])
		chunkIdx = runEnd
	}

	if chunkIdx < r.chunkCount {
		builder.appendRefsRange(r, chunkIdx, r.chunkCount)
	}

	root := builder.root()
	if root == nil {
		return FieldStorage{}
	}

	if root.keyCount < fieldIndexChunkThreshold {
		entries, data := root.flatten()
		flat := newFlatFieldStorage(entries, data)
		root.release()
		return flat
	}

	return newChunkedFieldStorage(root)
}

func (r *fieldIndexChunkedRoot) applyPostingDiffBuf(deltaKeys []PostingDelta) FieldStorage {
	if r == nil || r.keyCount == 0 {
		return applyFieldPostingDiffFlatMaybeChunkedBuf(nil, deltaKeys)
	}
	if deltaKeys == nil || len(deltaKeys) == 0 {
		return newChunkedFieldStorage(r)
	}
	if len(deltaKeys) == 1 {
		return r.applySinglePostingDiff(takePostingDeltaBuf(deltaKeys, 0))
	}

	builder := newFieldIndexChunkBuilder(r.keyCount + len(deltaKeys))
	chunkIdx := 0
	deltaPos := 0
	touchIdx := 0
	for deltaPos < len(deltaKeys) {
		touchIdx = r.touchChunkIndexFrom(max(touchIdx, chunkIdx), deltaKeys[deltaPos].Key)
		if touchIdx < 0 {
			break
		}
		if chunkIdx < touchIdx {
			builder.appendRefsRange(r, chunkIdx, touchIdx)
		}

		runStart := touchIdx
		runEnd := touchIdx + 1
		deltaStart := deltaPos
		deltaPos++
		for deltaPos < len(deltaKeys) {
			touchIdx = r.touchChunkIndexFrom(touchIdx, deltaKeys[deltaPos].Key)
			if touchIdx > runEnd {
				break
			}
			if touchIdx == runEnd {
				runEnd++
			}
			deltaPos++
		}

		builder.appendPostingDiffChunkRangeSortedBuf(r, runStart, runEnd, deltaKeys, deltaStart, deltaPos)
		chunkIdx = runEnd
	}

	if chunkIdx < r.chunkCount {
		builder.appendRefsRange(r, chunkIdx, r.chunkCount)
	}

	root := builder.root()
	if root == nil {
		return FieldStorage{}
	}

	if root.keyCount < fieldIndexChunkThreshold {
		entries, data := root.flatten()
		flat := newFlatFieldStorage(entries, data)
		root.release()
		return flat
	}

	return newChunkedFieldStorage(root)
}

func (r *fieldIndexChunkedRoot) rebuildWithOwnedPageRefsReplaced(page int, replRefs []fieldIndexChunkRef) *fieldIndexChunkedRoot {
	if r == nil || r.pages == nil || page < 0 || page >= len(r.pages) {
		if replRefs != nil {
			releaseOwnedFieldIndexChunkRefSlice(replRefs)
		}
		return nil
	}

	est := r.keyCount - r.pages[page].keyCount()
	if replRefs != nil {
		for i := range replRefs {
			if chunk := replRefs[i].chunk; chunk != nil {
				est += chunk.keyCount()
			}
		}
	}

	if est < 0 {
		est = 0
	}

	builder := newFieldIndexChunkBuilder(est)
	for i := 0; i < page; i++ {
		builder.appendPage(r.pages[i])
	}

	if replRefs != nil {
		for i := range replRefs {
			builder.appendOwnedRef(replRefs[i])
		}
		fieldIndexChunkRefSlicePool.Put(replRefs)
	}

	for i := page + 1; i < len(r.pages); i++ {
		builder.appendPage(r.pages[i])
	}

	return builder.root()
}

func (p *fieldIndexChunkDirPage) refsWithReplacedRef(off int, replRefs []fieldIndexChunkRef) []fieldIndexChunkRef {
	if p == nil || off < 0 || off >= len(p.refs) {
		if replRefs != nil {
			releaseOwnedFieldIndexChunkRefSlice(replRefs)
		}
		return nil
	}

	replLen := 0
	if replRefs != nil {
		replLen = len(replRefs)
	}

	total := len(p.refs) - 1 + replLen
	if total <= 0 {
		if replRefs != nil {
			releaseOwnedFieldIndexChunkRefSlice(replRefs)
		}
		return nil
	}

	refs := fieldIndexChunkRefSlicePool.Get(total)
	for i := 0; i < off; i++ {
		refs = append(refs, p.refs[i].retained())
	}

	for i := 0; i < replLen; i++ {
		refs = append(refs, replRefs[i])
		replRefs[i] = fieldIndexChunkRef{}
	}

	if replRefs != nil {
		fieldIndexChunkRefSlicePool.Put(replRefs)
	}

	for i := off + 1; i < len(p.refs); i++ {
		refs = append(refs, p.refs[i].retained())
	}

	return refs
}

func (r *fieldIndexChunkedRoot) applySinglePostingDiff(delta PostingDelta) FieldStorage {
	if r == nil || r.keyCount == 0 {
		return applyFieldPostingDiffFlatMaybeChunked(nil, []PostingDelta{delta})
	}

	touchIdx := r.touchChunkIndexFrom(0, delta.Key)
	if touchIdx < 0 {
		return newChunkedFieldStorage(r)
	}

	page, off := r.pagePosForChunk(touchIdx)
	if r.pages == nil || page >= len(r.pages) {
		return newChunkedFieldStorage(r)
	}

	pageRef := r.pages[page]

	ref, ok := r.refAtChunk(touchIdx)
	if !ok || ref.chunk == nil {
		return newChunkedFieldStorage(r)
	}

	entryIdx := ref.chunk.lowerBoundKey(delta.Key)

	if delta.Delta.Remove.IsEmpty() && !delta.Delta.Add.IsEmpty() && entryIdx < ref.chunk.keyCount() && keycodec.Compare(ref.chunk.keyAt(entryIdx), delta.Key) == 0 {

		baseIDs := ref.chunk.postingAt(entryIdx)
		updatedIDs := delta.Delta.applyOwned(baseIDs)
		if updatedIDs.SharesPayload(baseIDs) {
			return newChunkedFieldStorage(r)
		}

		rowsDelta := int64(updatedIDs.Cardinality()) - int64(baseIDs.Cardinality())

		rows := ref.chunk.rows
		if rowsDelta > 0 {
			rows += uint64(rowsDelta)
		} else if rowsDelta < 0 {
			rows -= uint64(-rowsDelta)
		}

		posts := fieldPostingSlice(ref.chunk.keyCount())
		for i := range posts {
			posts[i] = ref.chunk.postingAt(i)
		}

		posts[entryIdx] = updatedIDs
		var chunk *fieldIndexChunk

		if ref.chunk.hasNumericKeys() {
			keys := fieldUint64Slice(ref.chunk.keyCount())
			for i := range keys {
				keys[i] = ref.chunk.keyAt(i).U64()
			}
			chunk = newNumericFieldIndexChunk(posts, keys, rows)

		} else {
			refs := fieldStringRefSlice(len(ref.chunk.stringRefs))
			copy(refs, ref.chunk.stringRefs)
			data := fieldByteSlice(len(ref.chunk.stringData))
			copy(data, ref.chunk.stringData)
			chunk = newStringFieldIndexChunk(posts, refs, data, rows)
		}

		repl := fieldIndexChunkRefSlicePool.Get(1)
		repl = append(repl, fieldIndexChunkRef{
			last:  chunk.keyAt(chunk.keyCount() - 1),
			chunk: chunk,
		})

		root := r.rebuildWithOwnedPageRefsReplaced(page, pageRef.refsWithReplacedRef(off, repl))
		if root == nil {
			return FieldStorage{}
		}

		return newChunkedFieldStorage(root)
	}

	if delta.Delta.Remove.IsEmpty() && !delta.Delta.Add.IsEmpty() {
		replRefs := ref.chunk.refsWithInsertedEntry(entryIdx, Entry{
			Key: delta.Key,
			IDs: delta.Delta.Add,
		})

		if len(replRefs) > 0 {
			root := r.rebuildWithOwnedPageRefsReplaced(page, pageRef.refsWithReplacedRef(off, replRefs))
			if root == nil {
				return FieldStorage{}
			}
			if root.keyCount < fieldIndexChunkThreshold {
				entries, data := root.flatten()
				flat := newFlatFieldStorage(entries, data)
				root.release()
				return flat
			}
			return newChunkedFieldStorage(root)
		}

		if replRefs != nil {
			fieldIndexChunkRefSlicePool.Put(replRefs)
		}
	}

	entries := ref.chunk.borrowEntries()
	updated, same := applySingleFieldPostingDiffSorted(entries, delta)
	var replRefs []fieldIndexChunkRef
	if updated != nil {
		replRefs = newFieldIndexChunkRefsFromEntries(updated)
	}
	if updated != nil && !same {
		ReleaseFieldEntrySlice(updated)
	}
	ReleaseFieldEntrySlice(entries)

	root := r.rebuildWithOwnedPageRefsReplaced(page, pageRef.refsWithReplacedRef(off, replRefs))
	if root == nil {
		return FieldStorage{}
	}

	if root.keyCount < fieldIndexChunkThreshold {
		entries, data := root.flatten()
		flat := newFlatFieldStorage(entries, data)
		root.release()
		return flat
	}

	return newChunkedFieldStorage(root)
}
