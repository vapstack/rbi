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
	for i := range arena.values {
		arena.values[i].reset()
	}
	if cap(arena.values) > postingAccumMapMaxLen {
		arena.values = nil
	} else {
		arena.values = arena.values[:0]
	}
	postingDiffArenaPool.Put(arena)
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
	ref, ok := fieldDelta[key]
	if !ok {
		if *arena == nil {
			*arena = getPostingDiffArena(capHint)
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
	ref, ok := fieldDelta[key]
	if !ok {
		if *arena == nil {
			*arena = getPostingDiffArena(capHint)
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
	for i := range arena.values {
		arena.values[i].reset()
	}
	if cap(arena.values) > postingAccumMapMaxLen {
		arena.values = nil
	} else {
		arena.values = arena.values[:0]
	}
	postingAddArenaPool.Put(arena)
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
	pooled.PutUint64Slice(acc.spill)
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
		pooled.PutUint64Slice(acc.spill)
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
	ref, ok := fieldMap[key]
	if !ok {
		if *arena == nil {
			*arena = getPostingAddArena(capHint)
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
	ref, ok := fieldMap[key]
	if !ok {
		if *arena == nil {
			*arena = getPostingAddArena(capHint)
		}
		ref = (*arena).alloc()
		fieldMap[key] = ref
	}
	(*arena).accum(ref).add(idx)
	return fieldMap
}

func (delta *LenPostingDiff) putAfterMove() {
	if delta == nil {
		return
	}
	if delta.lengths != nil {
		PutBatchPostingDeltaMap(delta.lengths)
		delta.lengths = nil
	}
	lenPostingDiffPool.Put(delta)
}

func (delta *LenPostingDiff) Release() {
	if delta == nil {
		return
	}
	if delta.lengths != nil {
		for _, lengthDelta := range delta.lengths {
			lengthDelta.Add.Release()
			lengthDelta.Remove.Release()
		}
		PutBatchPostingDeltaMap(delta.lengths)
		delta.lengths = nil
	}
	delta.nonEmpty.Add.Release()
	delta.nonEmpty.Remove.Release()
	lenPostingDiffPool.Put(delta)
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
	delta.hasNonEmpty = true
	if isAdd {
		delta.nonEmpty.Add = delta.nonEmpty.Add.BuildAdded(idx)
	} else {
		delta.nonEmpty.Remove = delta.nonEmpty.Remove.BuildAdded(idx)
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

	buf := GetPostingDeltaSlice(len(deltas))
	for raw, ref := range deltas {
		delta := arena.accum(ref).materializeOwned()
		if delta.Add.IsEmpty() && delta.Remove.IsEmpty() {
			continue
		}
		buf = append(buf, PostingDelta{
			Key:   keycodec.FromStoredString(raw, fixed8),
			Delta: delta,
		})
	}
	if len(buf) == 0 {
		PutPostingDeltaSlice(buf)
		return nil
	}

	sortPostingDeltasBuf(buf)
	return buf
}

func sortedFixedPostingDeltasBufOwned(deltas map[uint64]uint32, arena *PostingDiffArena) []PostingDelta {
	if len(deltas) == 0 {
		return nil
	}

	buf := GetPostingDeltaSlice(len(deltas))
	for raw, ref := range deltas {
		delta := arena.accum(ref).materializeOwned()
		if delta.Add.IsEmpty() && delta.Remove.IsEmpty() {
			continue
		}
		buf = append(buf, PostingDelta{
			Key:   keycodec.FromU64(raw),
			Delta: delta,
		})
	}
	if len(buf) == 0 {
		PutPostingDeltaSlice(buf)
		return nil
	}

	sortPostingDeltasBuf(buf)
	return buf
}

func (base FieldStorage) ApplyStringPostingDiffOwned(
	deltas map[string]uint32,
	arena *PostingDiffArena,
	fixed8 bool,
	allowChunk bool,
) FieldStorage {
	defer PutStringPostingDiffMap(deltas)
	buf := sortedStringPostingDeltasBufOwned(deltas, arena, fixed8)
	if buf == nil {
		return base
	}
	return base.applyPostingDiffBufOwned(buf, allowChunk)
}

func (base FieldStorage) ApplyFixedPostingDiffOwned(
	deltas map[uint64]uint32,
	arena *PostingDiffArena,
	allowChunk bool,
) FieldStorage {
	defer PutFixedPostingDiffMap(deltas)
	buf := sortedFixedPostingDeltasBufOwned(deltas, arena)
	if buf == nil {
		return base
	}
	return base.applyPostingDiffBufOwned(buf, allowChunk)
}

func sortedStringPostingAddsBufOwned(adds map[string]uint32, arena *PostingAddArena, fixed8 bool) []PostingDelta {
	if len(adds) == 0 {
		return nil
	}
	buf := GetPostingDeltaSlice(len(adds))
	for raw, ref := range adds {
		ids := arena.accum(ref).materializeOwned()
		buf = append(buf, PostingDelta{
			Key: keycodec.FromStoredString(raw, fixed8),
			Delta: BatchPostingDelta{
				Add: ids,
			},
		})
	}
	sortPostingDeltasBuf(buf)
	return buf
}

func sortedFixedPostingAddsBufOwned(adds map[uint64]uint32, arena *PostingAddArena) []PostingDelta {
	if len(adds) == 0 {
		return nil
	}
	buf := GetPostingDeltaSlice(len(adds))
	for raw, ref := range adds {
		ids := arena.accum(ref).materializeOwned()
		buf = append(buf, PostingDelta{
			Key: keycodec.FromU64(raw),
			Delta: BatchPostingDelta{
				Add: ids,
			},
		})
	}
	sortPostingDeltasBuf(buf)
	return buf
}

func (base FieldStorage) MergeStringPostingAddsOwned(
	adds map[string]uint32,
	arena *PostingAddArena,
	fixed8 bool,
	allowChunk bool,
) FieldStorage {
	defer PutStringPostingAddMap(adds)
	if len(adds) == 0 {
		return base
	}
	buf := sortedStringPostingAddsBufOwned(adds, arena, fixed8)
	if buf == nil {
		return base
	}
	return base.applyPostingDiffBufOwned(buf, allowChunk)
}

func (base FieldStorage) MergeFixedPostingAddsOwned(
	adds map[uint64]uint32,
	arena *PostingAddArena,
	allowChunk bool,
) FieldStorage {
	defer PutFixedPostingAddMap(adds)
	if len(adds) == 0 {
		return base
	}
	buf := sortedFixedPostingAddsBufOwned(adds, arena)
	if buf == nil {
		return base
	}
	return base.applyPostingDiffBufOwned(buf, allowChunk)
}

func (deltas *LenPostingDiff) sortedBufOwned() []PostingDelta {
	if deltas == nil {
		return nil
	}
	count := len(deltas.lengths)
	if deltas.hasNonEmpty {
		count++
	}
	if count == 0 {
		deltas.putAfterMove()
		return nil
	}

	buf := GetPostingDeltaSlice(count)
	for raw, delta := range deltas.lengths {
		if delta.Add.IsEmpty() && delta.Remove.IsEmpty() {
			continue
		}
		buf = append(buf, PostingDelta{
			Key:   keycodec.FromU64(raw),
			Delta: delta,
		})
	}
	if deltas.hasNonEmpty && (!deltas.nonEmpty.Add.IsEmpty() || !deltas.nonEmpty.Remove.IsEmpty()) {
		buf = append(buf, PostingDelta{
			Key:   keycodec.FromString(LenIndexNonEmptyKey),
			Delta: deltas.nonEmpty,
		})
	}
	deltas.putAfterMove()
	if len(buf) == 0 {
		PutPostingDeltaSlice(buf)
		return nil
	}
	sortPostingDeltasBuf(buf)
	return buf
}

func (deltas *LenPostingDiff) count() int {
	if deltas == nil {
		return 0
	}
	count := 0
	for _, delta := range deltas.lengths {
		if delta.Add.IsEmpty() && delta.Remove.IsEmpty() {
			continue
		}
		count++
	}
	if deltas.hasNonEmpty && (!deltas.nonEmpty.Add.IsEmpty() || !deltas.nonEmpty.Remove.IsEmpty()) {
		count++
	}
	return count
}

func (deltas *LenPostingDiff) takeOwned(dst []PostingDelta) int {
	if deltas == nil || len(dst) == 0 {
		return 0
	}
	n := 0
	for raw, delta := range deltas.lengths {
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
	if n < len(dst) && deltas.hasNonEmpty && (!deltas.nonEmpty.Add.IsEmpty() || !deltas.nonEmpty.Remove.IsEmpty()) {
		dst[n] = PostingDelta{
			Key:   keycodec.FromString(LenIndexNonEmptyKey),
			Delta: deltas.nonEmpty,
		}
		n++
	}
	deltas.putAfterMove()
	return n
}

func (base FieldStorage) ApplyLenPostingDiffOwned(deltas *LenPostingDiff) FieldStorage {
	count := deltas.count()
	if count == 0 {
		deltas.putAfterMove()
		return base
	}
	if count == 1 {
		delta, ok := deltas.takeSingleOwned()
		if !ok {
			return base
		}
		storage := base.applyPostingDiff([]PostingDelta{delta}, base.IsChunked())
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
		deltaKeys = inline[:n]
		if len(deltaKeys) > 1 && keycodec.Compare(deltaKeys[0].Key, deltaKeys[1].Key) > 0 {
			deltaKeys[0], deltaKeys[1] = deltaKeys[1], deltaKeys[0]
		}
	} else {
		buf = deltas.sortedBufOwned()
		if buf == nil {
			return base
		}
	}

	var storage FieldStorage
	if buf != nil {
		storage = base.applyPostingDiffBufOwned(buf, base.IsChunked())
	} else {
		storage = base.applyPostingDiff(deltaKeys, base.IsChunked())
	}
	if storage.KeyCount() > 0 {
		return storage
	}
	return FieldStorage{}
}

func (deltas *LenPostingDiff) takeSingleOwned() (PostingDelta, bool) {
	if deltas == nil {
		return PostingDelta{}, false
	}
	for raw, delta := range deltas.lengths {
		if delta.Add.IsEmpty() && delta.Remove.IsEmpty() {
			continue
		}
		deltas.putAfterMove()
		return PostingDelta{
			Key:   keycodec.FromU64(raw),
			Delta: delta,
		}, true
	}
	if deltas.hasNonEmpty && (!deltas.nonEmpty.Add.IsEmpty() || !deltas.nonEmpty.Remove.IsEmpty()) {
		delta := deltas.nonEmpty
		deltas.putAfterMove()
		return PostingDelta{
			Key:   keycodec.FromString(LenIndexNonEmptyKey),
			Delta: delta,
		}, true
	}
	deltas.putAfterMove()
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

func (base *fieldIndexChunkedRoot) nextPostingDiffEntry(endChunk, chunkIdx, entryIdx int) (Entry, bool) {
	for chunkIdx < endChunk {
		ref, _ := base.refAtChunk(chunkIdx)
		chunk := ref.chunk
		if entryIdx < chunk.keyCount() {
			return Entry{Key: chunk.keyAt(entryIdx), IDs: chunk.postingAt(entryIdx)}, true
		}
		chunkIdx++
		entryIdx = 0
	}
	return Entry{}, false
}

func (base *fieldIndexChunkedRoot) advancePostingDiffEntry(endChunk int, chunkIdx *int, entryIdx *int) {
	*entryIdx += 1
	for *chunkIdx < endChunk {
		ref, _ := base.refAtChunk(*chunkIdx)
		if *entryIdx < ref.chunk.keyCount() {
			return
		}
		*chunkIdx += 1
		*entryIdx = 0
	}
}

func (delta BatchPostingDelta) mutationCardinality() uint64 {
	return delta.Add.Cardinality() + delta.Remove.Cardinality()
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

func (delta BatchPostingDelta) shouldOptimize(base posting.List, out posting.List) bool {
	if out.IsEmpty() {
		return false
	}
	if _, ok := out.TrySingle(); ok {
		return false
	}
	if postingListLooksRunFriendly(delta.Add) || postingListLooksRunFriendly(delta.Remove) || postingListLooksRunFriendly(out) {
		return true
	}
	if base.IsEmpty() {
		return false
	}
	if _, ok := base.TrySingle(); ok {
		return false
	}
	mutation := delta.mutationCardinality()
	if mutation == 0 {
		return false
	}
	baseCard := base.Cardinality()
	return baseCard > 0 && mutation*4 >= baseCard
}

func (delta *BatchPostingDelta) applyOwned(base posting.List) posting.List {
	deltaValue := *delta
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
	delta.Remove = posting.List{}
	delta.Add = posting.List{}
	return out
}

func applyFieldPostingDiffSorted(base *[]Entry, deltaKeys []PostingDelta) *[]Entry {
	if len(deltaKeys) == 0 {
		return base
	}
	if len(deltaKeys) == 1 {
		return applySingleFieldPostingDiffSorted(base, deltaKeys[0])
	}

	var src []Entry
	if base != nil {
		src = *base
	}
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
		PutFieldEntrySlice(out)
		return nil
	}

	return &out
}

func applyFieldPostingDiffSortedBuf(base *[]Entry, deltaKeys []PostingDelta) *[]Entry {
	if deltaKeys == nil || len(deltaKeys) == 0 {
		return base
	}
	if len(deltaKeys) == 1 {
		return applySingleFieldPostingDiffSorted(base, takePostingDeltaBuf(deltaKeys, 0))
	}

	var src []Entry
	if base != nil {
		src = *base
	}
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
		PutFieldEntrySlice(out)
		return nil
	}
	return &out
}

func applySingleFieldPostingDiffSorted(base *[]Entry, delta PostingDelta) *[]Entry {
	var src []Entry
	if base != nil {
		src = *base
	}
	if len(src) == 0 {
		ids := delta.Delta.applyOwned(posting.List{})
		if ids.IsEmpty() {
			return nil
		}
		out := GetFieldEntrySlice(1)
		out = append(out, Entry{Key: delta.Key, IDs: ids})
		return &out
	}

	pos := lowerBoundIndexEntriesKey(src, delta.Key)
	if pos >= len(src) || keycodec.Compare(src[pos].Key, delta.Key) > 0 {
		ids := delta.Delta.applyOwned(posting.List{})
		if ids.IsEmpty() {
			return base
		}
		out := GetFieldEntrySlice(len(src) + 1)[:len(src)+1]
		copyBorrowedIndexEntries(out[:pos], src[:pos])
		out[pos] = Entry{Key: delta.Key, IDs: ids}
		copyBorrowedIndexEntries(out[pos+1:], src[pos:])
		return &out
	}

	ids := delta.Delta.applyOwned(src[pos].IDs)
	if ids.SharesPayload(src[pos].IDs) {
		return base
	}

	if ids.IsEmpty() {
		if len(src) == 1 {
			return nil
		}
		out := GetFieldEntrySlice(len(src) - 1)[:len(src)-1]
		copyBorrowedIndexEntries(out[:pos], src[:pos])
		copyBorrowedIndexEntries(out[pos:], src[pos+1:])
		return &out
	}

	out := GetFieldEntrySlice(len(src))[:len(src)]
	copyBorrowedIndexEntries(out, src)
	out[pos].IDs = ids
	return &out
}

func (builder *fieldIndexChunkBuilder) appendPostingDiffFlatSorted(base *[]Entry, deltaKeys []PostingDelta) {
	if builder == nil {
		return
	}

	var src []Entry
	if base != nil {
		src = *base
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
	out := newFieldIndexChunkStreamBuilder(builder, numeric)

	i, j := 0, 0
	for i < len(src) || j < len(deltaKeys) {
		switch {

		case j >= len(deltaKeys):
			out.append(src[i].Key, src[i].IDs.Borrow())
			i++

		case i >= len(src):
			ids := deltaKeys[j].Delta.applyOwned(posting.List{})
			if !ids.IsEmpty() {
				out.append(deltaKeys[j].Key, ids)
			}
			j++

		default:
			cmp := keycodec.Compare(src[i].Key, deltaKeys[j].Key)
			switch {

			case cmp < 0:
				out.append(src[i].Key, src[i].IDs.Borrow())
				i++

			case cmp > 0:
				ids := deltaKeys[j].Delta.applyOwned(posting.List{})
				if !ids.IsEmpty() {
					out.append(deltaKeys[j].Key, ids)
				}
				j++

			default:
				ids := deltaKeys[j].Delta.applyOwned(src[i].IDs)
				if !ids.IsEmpty() {
					if ids.SharesPayload(src[i].IDs) {
						out.append(src[i].Key, src[i].IDs.Borrow())
					} else {
						out.append(src[i].Key, ids)
					}
				}
				i++
				j++
			}
		}
	}

	out.finish()
}

func (builder *fieldIndexChunkBuilder) appendPostingDiffFlatSortedBuf(base *[]Entry, deltaKeys []PostingDelta) {
	if builder == nil {
		return
	}

	var src []Entry
	if base != nil {
		src = *base
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
	out := newFieldIndexChunkStreamBuilder(builder, numeric)

	i, j := 0, 0
	for i < len(src) || (deltaKeys != nil && j < len(deltaKeys)) {
		switch {

		case deltaKeys == nil || j >= len(deltaKeys):
			out.append(src[i].Key, src[i].IDs.Borrow())
			i++

		case i >= len(src):
			delta := takePostingDeltaBuf(deltaKeys, j)
			ids := delta.Delta.applyOwned(posting.List{})
			if !ids.IsEmpty() {
				out.append(delta.Key, ids)
			}
			j++

		default:
			delta := deltaKeys[j]
			cmp := keycodec.Compare(src[i].Key, delta.Key)
			switch {

			case cmp < 0:
				out.append(src[i].Key, src[i].IDs.Borrow())
				i++

			case cmp > 0:
				delta = takePostingDeltaBuf(deltaKeys, j)
				ids := delta.Delta.applyOwned(posting.List{})
				if !ids.IsEmpty() {
					out.append(delta.Key, ids)
				}
				j++

			default:
				delta = takePostingDeltaBuf(deltaKeys, j)
				ids := delta.Delta.applyOwned(src[i].IDs)
				if !ids.IsEmpty() {
					if ids.SharesPayload(src[i].IDs) {
						out.append(src[i].Key, src[i].IDs.Borrow())
					} else {
						out.append(src[i].Key, ids)
					}
				}
				i++
				j++
			}
		}
	}

	out.finish()
}

func applyFieldPostingDiffFlatMaybeChunked(base *[]Entry, deltaKeys []PostingDelta) FieldStorage {
	est := len(deltaKeys)
	if base != nil {
		est += len(*base)
	}
	builder := newFieldIndexChunkBuilder(est)
	builder.appendPostingDiffFlatSorted(base, deltaKeys)

	root := builder.root()
	if root == nil {
		return FieldStorage{}
	}

	if root.keyCount < fieldIndexChunkThreshold {
		flat := newFlatFieldStorage(root.flatten())
		root.release()
		return flat
	}

	return newChunkedFieldStorage(root)
}

func applyFieldPostingDiffFlatMaybeChunkedBuf(base *[]Entry, deltaKeys []PostingDelta) FieldStorage {
	est := 0
	if deltaKeys != nil {
		est = len(deltaKeys)
	}
	if base != nil {
		est += len(*base)
	}

	builder := newFieldIndexChunkBuilder(est)
	builder.appendPostingDiffFlatSortedBuf(base, deltaKeys)

	root := builder.root()
	if root == nil {
		return FieldStorage{}
	}

	if root.keyCount < fieldIndexChunkThreshold {
		flat := newFlatFieldStorage(root.flatten())
		root.release()
		return flat
	}

	return newChunkedFieldStorage(root)
}

func (base FieldStorage) applyPostingDiff(deltaKeys []PostingDelta, allowChunk bool) FieldStorage {
	if len(deltaKeys) == 0 {
		return base
	}

	if !allowChunk {
		var flat *[]Entry
		if base.flat != nil {
			flat = &base.flat.entries
		}
		updated := applyFieldPostingDiffSorted(flat, deltaKeys)
		if updated == flat {
			return base
		}
		return newFlatFieldStorage(updated)
	}

	if base.chunked != nil {
		return base.chunked.applyPostingDiff(deltaKeys)
	}

	var flat *[]Entry
	if base.flat != nil {
		flat = &base.flat.entries
	}

	baseCount := 0
	if flat != nil {
		baseCount = len(*flat)
	}

	if len(deltaKeys)+baseCount >= fieldIndexChunkThreshold {
		return applyFieldPostingDiffFlatMaybeChunked(flat, deltaKeys)
	}

	updated := applyFieldPostingDiffSorted(flat, deltaKeys)
	if updated == flat {
		return base
	}

	return newRegularFieldStorage(updated)
}

func (base FieldStorage) applyPostingDiffBufOwned(buf []PostingDelta, allowChunk bool) FieldStorage {
	if buf == nil {
		return base
	}
	defer PutPostingDeltaSlice(buf)

	if len(buf) == 0 {
		return base
	}

	if !allowChunk {
		var flat *[]Entry
		if base.flat != nil {
			flat = &base.flat.entries
		}
		updated := applyFieldPostingDiffSortedBuf(flat, buf)
		if updated == flat {
			return base
		}
		return newFlatFieldStorage(updated)
	}

	if base.chunked != nil {
		return base.chunked.applyPostingDiffBuf(buf)
	}

	var flat *[]Entry
	if base.flat != nil {
		flat = &base.flat.entries
	}

	baseCount := 0
	if flat != nil {
		baseCount = len(*flat)
	}

	if len(buf)+baseCount >= fieldIndexChunkThreshold {
		return applyFieldPostingDiffFlatMaybeChunkedBuf(flat, buf)
	}

	updated := applyFieldPostingDiffSortedBuf(flat, buf)
	if updated == flat {
		return base
	}

	return newRegularFieldStorage(updated)
}

func (builder *fieldIndexChunkBuilder) appendPostingDiffChunkRangeSorted(
	base *fieldIndexChunkedRoot,
	startChunk, endChunk int,
	deltaKeys []PostingDelta,
) {
	if builder == nil || base == nil || startChunk < 0 || endChunk < startChunk || endChunk > base.chunkCount {
		return
	}

	if len(deltaKeys) == 0 {
		builder.appendRefsRange(base, startChunk, endChunk)
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

	out := newFieldIndexChunkStreamBuilder(builder, numeric)

	chunkIdx := startChunk
	entryIdx := 0
	j := 0

	for {
		baseEnt, hasBase := base.nextPostingDiffEntry(endChunk, chunkIdx, entryIdx)
		switch {

		case !hasBase && j >= len(deltaKeys):
			out.finish()
			return

		case !hasBase:
			ids := deltaKeys[j].Delta.applyOwned(posting.List{})
			if !ids.IsEmpty() {
				out.append(deltaKeys[j].Key, ids)
			}
			j++

		case j >= len(deltaKeys):
			out.append(baseEnt.Key, baseEnt.IDs)
			base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)

		default:
			cmp := keycodec.Compare(baseEnt.Key, deltaKeys[j].Key)
			switch {

			case cmp < 0:
				out.append(baseEnt.Key, baseEnt.IDs)
				base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)

			case cmp > 0:
				ids := deltaKeys[j].Delta.applyOwned(posting.List{})
				if !ids.IsEmpty() {
					out.append(deltaKeys[j].Key, ids)
				}
				j++

			default:
				ids := deltaKeys[j].Delta.applyOwned(baseEnt.IDs)
				if !ids.IsEmpty() {
					out.append(baseEnt.Key, ids)
				}
				base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)
				j++
			}
		}
	}
}

func (builder *fieldIndexChunkBuilder) appendPostingDiffChunkRangeSortedBuf(
	base *fieldIndexChunkedRoot,
	startChunk, endChunk int,
	deltaKeys []PostingDelta,
	deltaStart, deltaEnd int,
) {
	if builder == nil || base == nil || startChunk < 0 || endChunk < startChunk || endChunk > base.chunkCount {
		return
	}

	if deltaKeys == nil || deltaStart >= deltaEnd {
		builder.appendRefsRange(base, startChunk, endChunk)
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

	out := newFieldIndexChunkStreamBuilder(builder, numeric)

	chunkIdx := startChunk
	entryIdx := 0
	j := deltaStart

	for {
		baseEnt, hasBase := base.nextPostingDiffEntry(endChunk, chunkIdx, entryIdx)

		switch {
		case !hasBase && j >= deltaEnd:
			out.finish()
			return

		case !hasBase:
			delta := takePostingDeltaBuf(deltaKeys, j)
			ids := delta.Delta.applyOwned(posting.List{})
			if !ids.IsEmpty() {
				out.append(delta.Key, ids)
			}
			j++

		case j >= deltaEnd:
			out.append(baseEnt.Key, baseEnt.IDs)
			base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)

		default:
			delta := deltaKeys[j]
			cmp := keycodec.Compare(baseEnt.Key, delta.Key)

			switch {
			case cmp < 0:
				out.append(baseEnt.Key, baseEnt.IDs)
				base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)

			case cmp > 0:
				delta = takePostingDeltaBuf(deltaKeys, j)
				ids := delta.Delta.applyOwned(posting.List{})
				if !ids.IsEmpty() {
					out.append(delta.Key, ids)
				}
				j++

			default:
				delta = takePostingDeltaBuf(deltaKeys, j)
				ids := delta.Delta.applyOwned(baseEnt.IDs)
				if !ids.IsEmpty() {
					out.append(baseEnt.Key, ids)
				}
				base.advancePostingDiffEntry(endChunk, &chunkIdx, &entryIdx)
				j++
			}
		}
	}
}

func (base *fieldIndexChunkedRoot) applyPostingDiff(deltaKeys []PostingDelta) FieldStorage {
	if base == nil || base.keyCount == 0 {
		return applyFieldPostingDiffFlatMaybeChunked(nil, deltaKeys)
	}
	if len(deltaKeys) == 0 {
		return newChunkedFieldStorage(base)
	}
	if len(deltaKeys) == 1 {
		return base.applySinglePostingDiff(deltaKeys[0])
	}

	builder := newFieldIndexChunkBuilder(base.keyCount + len(deltaKeys))
	chunkIdx := 0
	deltaPos := 0
	touchIdx := 0

	for deltaPos < len(deltaKeys) {
		touchIdx = base.touchChunkIndexFrom(max(touchIdx, chunkIdx), deltaKeys[deltaPos].Key)
		if touchIdx < 0 {
			break
		}
		if chunkIdx < touchIdx {
			builder.appendRefsRange(base, chunkIdx, touchIdx)
		}

		runStart := touchIdx
		runEnd := touchIdx + 1
		deltaStart := deltaPos
		deltaPos++
		for deltaPos < len(deltaKeys) {
			touchIdx = base.touchChunkIndexFrom(touchIdx, deltaKeys[deltaPos].Key)
			if touchIdx > runEnd {
				break
			}
			if touchIdx == runEnd {
				runEnd++
			}
			deltaPos++
		}

		builder.appendPostingDiffChunkRangeSorted(base, runStart, runEnd, deltaKeys[deltaStart:deltaPos])
		chunkIdx = runEnd
	}

	if chunkIdx < base.chunkCount {
		builder.appendRefsRange(base, chunkIdx, base.chunkCount)
	}

	root := builder.root()
	if root == nil {
		return FieldStorage{}
	}

	if root.keyCount < fieldIndexChunkThreshold {
		flat := newFlatFieldStorage(root.flatten())
		root.release()
		return flat
	}

	return newChunkedFieldStorage(root)
}

func (base *fieldIndexChunkedRoot) applyPostingDiffBuf(deltaKeys []PostingDelta) FieldStorage {
	if base == nil || base.keyCount == 0 {
		return applyFieldPostingDiffFlatMaybeChunkedBuf(nil, deltaKeys)
	}
	if deltaKeys == nil || len(deltaKeys) == 0 {
		return newChunkedFieldStorage(base)
	}
	if len(deltaKeys) == 1 {
		return base.applySinglePostingDiff(takePostingDeltaBuf(deltaKeys, 0))
	}

	builder := newFieldIndexChunkBuilder(base.keyCount + len(deltaKeys))
	chunkIdx := 0
	deltaPos := 0
	touchIdx := 0
	for deltaPos < len(deltaKeys) {
		touchIdx = base.touchChunkIndexFrom(max(touchIdx, chunkIdx), deltaKeys[deltaPos].Key)
		if touchIdx < 0 {
			break
		}
		if chunkIdx < touchIdx {
			builder.appendRefsRange(base, chunkIdx, touchIdx)
		}

		runStart := touchIdx
		runEnd := touchIdx + 1
		deltaStart := deltaPos
		deltaPos++
		for deltaPos < len(deltaKeys) {
			touchIdx = base.touchChunkIndexFrom(touchIdx, deltaKeys[deltaPos].Key)
			if touchIdx > runEnd {
				break
			}
			if touchIdx == runEnd {
				runEnd++
			}
			deltaPos++
		}

		builder.appendPostingDiffChunkRangeSortedBuf(base, runStart, runEnd, deltaKeys, deltaStart, deltaPos)
		chunkIdx = runEnd
	}

	if chunkIdx < base.chunkCount {
		builder.appendRefsRange(base, chunkIdx, base.chunkCount)
	}

	root := builder.root()
	if root == nil {
		return FieldStorage{}
	}

	if root.keyCount < fieldIndexChunkThreshold {
		flat := newFlatFieldStorage(root.flatten())
		root.release()
		return flat
	}

	return newChunkedFieldStorage(root)
}

func (base *fieldIndexChunkedRoot) rebuildWithOwnedPageRefsReplaced(page int, replRefs []fieldIndexChunkRef) *fieldIndexChunkedRoot {
	if base == nil || base.pages == nil || page < 0 || page >= len(base.pages) {
		if replRefs != nil {
			putOwnedFieldIndexChunkRefSlice(replRefs)
		}
		return nil
	}

	est := base.keyCount - base.pages[page].keyCount()
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
		builder.appendPage(base.pages[i])
	}

	if replRefs != nil {
		for i := range replRefs {
			builder.appendOwnedRef(replRefs[i])
		}
		fieldIndexChunkRefSlicePool.Put(replRefs)
	}

	for i := page + 1; i < len(base.pages); i++ {
		builder.appendPage(base.pages[i])
	}

	return builder.root()
}

func (page *fieldIndexChunkDirPage) refsWithReplacedRef(off int, replRefs []fieldIndexChunkRef) []fieldIndexChunkRef {
	if page == nil || off < 0 || off >= len(page.refs) {
		if replRefs != nil {
			putOwnedFieldIndexChunkRefSlice(replRefs)
		}
		return nil
	}

	replLen := 0
	if replRefs != nil {
		replLen = len(replRefs)
	}

	total := len(page.refs) - 1 + replLen
	if total <= 0 {
		if replRefs != nil {
			putOwnedFieldIndexChunkRefSlice(replRefs)
		}
		return nil
	}

	refs := fieldIndexChunkRefSlicePool.Get(total)
	for i := 0; i < off; i++ {
		refs = append(refs, page.refs[i].retained())
	}

	for i := 0; i < replLen; i++ {
		refs = append(refs, replRefs[i])
		replRefs[i] = fieldIndexChunkRef{}
	}

	if replRefs != nil {
		fieldIndexChunkRefSlicePool.Put(replRefs)
	}

	for i := off + 1; i < len(page.refs); i++ {
		refs = append(refs, page.refs[i].retained())
	}

	return refs
}

func (base *fieldIndexChunkedRoot) applySinglePostingDiff(delta PostingDelta) FieldStorage {
	if base == nil || base.keyCount == 0 {
		return applyFieldPostingDiffFlatMaybeChunked(nil, []PostingDelta{delta})
	}

	touchIdx := base.touchChunkIndexFrom(0, delta.Key)
	if touchIdx < 0 {
		return newChunkedFieldStorage(base)
	}

	page, off := base.pagePosForChunk(touchIdx)
	if base.pages == nil || page >= len(base.pages) {
		return newChunkedFieldStorage(base)
	}

	pageRef := base.pages[page]

	ref, ok := base.refAtChunk(touchIdx)
	if !ok || ref.chunk == nil {
		return newChunkedFieldStorage(base)
	}

	entryIdx := ref.chunk.lowerBoundKey(delta.Key)

	if delta.Delta.Remove.IsEmpty() && !delta.Delta.Add.IsEmpty() && entryIdx < ref.chunk.keyCount() && keycodec.Compare(ref.chunk.keyAt(entryIdx), delta.Key) == 0 {

		baseIDs := ref.chunk.postingAt(entryIdx)
		updatedIDs := delta.Delta.applyOwned(baseIDs)
		if updatedIDs.SharesPayload(baseIDs) {
			return newChunkedFieldStorage(base)
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

		root := base.rebuildWithOwnedPageRefsReplaced(page, pageRef.refsWithReplacedRef(off, repl))
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
			root := base.rebuildWithOwnedPageRefsReplaced(page, pageRef.refsWithReplacedRef(off, replRefs))
			if root == nil {
				return FieldStorage{}
			}
			if root.keyCount < fieldIndexChunkThreshold {
				flat := newFlatFieldStorage(root.flatten())
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
	updated := applySingleFieldPostingDiffSorted(&entries, delta)
	var replRefs []fieldIndexChunkRef
	if updated != nil {
		replRefs = newFieldIndexChunkRefsFromEntries(*updated)
	}
	if updated != nil && updated != &entries {
		PutFieldEntrySlice(*updated)
	}
	PutFieldEntrySlice(entries)

	root := base.rebuildWithOwnedPageRefsReplaced(page, pageRef.refsWithReplacedRef(off, replRefs))
	if root == nil {
		return FieldStorage{}
	}

	if root.keyCount < fieldIndexChunkThreshold {
		flat := newFlatFieldStorage(root.flatten())
		root.release()
		return flat
	}

	return newChunkedFieldStorage(root)
}
