package indexdata

import (
	"strings"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

type Bounds struct {
	Has bool

	Empty bool

	HasLo     bool
	LoKey     string
	LoIndex   keycodec.IndexKey
	LoNumeric bool
	LoInc     bool

	HasHi     bool
	HiKey     string
	HiIndex   keycodec.IndexKey
	HiNumeric bool
	HiInc     bool

	HasPrefix bool
	Prefix    string
}

func (b *Bounds) SetEmpty() {
	b.Has = true
	b.Empty = true
}

func (b *Bounds) Normalize() {
	if b.Empty {
		return
	}
	if b.HasLo && !b.LoNumeric {
		b.LoIndex = keycodec.FromString(b.LoKey)
	}
	if b.HasHi && !b.HiNumeric {
		b.HiIndex = keycodec.FromString(b.HiKey)
	}

	if b.HasLo && b.HasHi {
		cmp := b.compareLoHi()
		if cmp > 0 || (cmp == 0 && (!b.LoInc || !b.HiInc)) {
			b.SetEmpty()
			return
		}
	}

	if !b.HasPrefix {
		return
	}

	if b.HasHi {
		cmp := keycodec.CompareString(b.hiIndex(), b.Prefix)
		if cmp < 0 || (cmp == 0 && !b.HiInc) {
			b.SetEmpty()
			return
		}
	}

	if b.HasLo {
		if upper, ok := keycodec.NewPrefixUpperBound(b.Prefix); ok {
			cmp := keycodec.ComparePrefixUpperBound(b.loIndex(), upper)
			if cmp >= 0 {
				b.SetEmpty()
			}
		}
	}
}

func (b Bounds) IsPointRange() bool {
	return !b.HasPrefix && b.HasLo && b.HasHi && b.LoInc && b.HiInc && b.compareLoHi() == 0
}

func (b Bounds) loIndex() keycodec.IndexKey {
	if b.LoNumeric {
		return b.LoIndex
	}
	return keycodec.FromString(b.LoKey)
}

func (b Bounds) hiIndex() keycodec.IndexKey {
	if b.HiNumeric {
		return b.HiIndex
	}
	return keycodec.FromString(b.HiKey)
}

func (b Bounds) compareLoHi() int {
	return keycodec.Compare(b.loIndex(), b.hiIndex())
}

func (b *Bounds) ApplyLo(key string, inc bool) {
	if b.Empty {
		return
	}
	idxKey := keycodec.FromString(key)
	cmp := -1
	if b.HasLo {
		cmp = keycodec.Compare(b.loIndex(), idxKey)
	}
	if !b.HasLo || cmp < 0 || (cmp == 0 && b.LoInc && !inc) {
		b.HasLo = true
		b.LoKey = key
		b.LoIndex = idxKey
		b.LoNumeric = false
		b.LoInc = inc
	}
	b.Normalize()
}

func (b *Bounds) ApplyLoIndex(key keycodec.IndexKey, inc bool) {
	if b.Empty {
		return
	}
	cmp := -1
	if b.HasLo {
		cmp = keycodec.Compare(b.loIndex(), key)
	}
	if !b.HasLo || cmp < 0 || (cmp == 0 && b.LoInc && !inc) {
		b.HasLo = true
		b.LoKey = ""
		b.LoIndex = key
		b.LoNumeric = true
		b.LoInc = inc
	}
	b.Normalize()
}

func (b *Bounds) ApplyHi(key string, inc bool) {
	if b.Empty {
		return
	}
	idxKey := keycodec.FromString(key)
	cmp := 1
	if b.HasHi {
		cmp = keycodec.Compare(b.hiIndex(), idxKey)
	}
	if !b.HasHi || cmp > 0 || (cmp == 0 && b.HiInc && !inc) {
		b.HasHi = true
		b.HiKey = key
		b.HiIndex = idxKey
		b.HiNumeric = false
		b.HiInc = inc
	}
	b.Normalize()
}

func (b *Bounds) ApplyHiIndex(key keycodec.IndexKey, inc bool) {
	if b.Empty {
		return
	}
	cmp := 1
	if b.HasHi {
		cmp = keycodec.Compare(b.hiIndex(), key)
	}
	if !b.HasHi || cmp > 0 || (cmp == 0 && b.HiInc && !inc) {
		b.HasHi = true
		b.HiKey = ""
		b.HiIndex = key
		b.HiNumeric = true
		b.HiInc = inc
	}
	b.Normalize()
}

func (b *Bounds) ApplyPrefix(prefix string) {
	if b.Empty {
		return
	}
	if b.HasPrefix {
		switch {
		case strings.HasPrefix(b.Prefix, prefix):
		case strings.HasPrefix(prefix, b.Prefix):
			b.Prefix = prefix
		default:
			b.SetEmpty()
			return
		}
	} else {
		b.HasPrefix = true
		b.Prefix = prefix
	}
	b.Normalize()
}

// FieldIndexView provides read helpers over one immutable field storage view.
type FieldIndexView struct {
	base    []Entry
	chunked *fieldIndexChunkedRoot
}

type FieldIndexRange struct {
	BaseStart int
	BaseEnd   int

	startPos fieldIndexChunkPos
	endPos   fieldIndexChunkPos

	filterPrefix    string
	filterPrefixSet bool
}

type FieldIndexCursor struct {
	base    []Entry
	chunked *fieldIndexChunkedRoot

	pos       int
	end       int
	remaining int

	chunkIdx int
	entryIdx int
	pageIdx  int
	refIdx   int
	chunk    *fieldIndexChunk
	desc     bool

	filterPrefix    string
	filterPrefixSet bool
}

func (br FieldIndexRange) Empty() bool {
	return br.BaseStart >= br.BaseEnd
}

func (br FieldIndexRange) Len() int {
	if br.BaseStart >= br.BaseEnd {
		return 0
	}
	return br.BaseEnd - br.BaseStart
}

// ExactRankSpan reports whether every rank in [BaseStart, BaseEnd) belongs to the range.
func (br FieldIndexRange) ExactRankSpan() bool {
	return !br.filterPrefixSet
}

func NewFieldIndexViewFromStorage(storage FieldStorage) FieldIndexView {
	if storage.chunked != nil {
		return FieldIndexView{chunked: storage.chunked}
	}
	if storage.flat != nil {
		return FieldIndexView{base: storage.flat.entries}
	}
	return FieldIndexView{}
}

func (o FieldIndexView) HasData() bool {
	return o.KeyCount() > 0
}

func (o FieldIndexView) KeyCount() int {
	if o.chunked != nil {
		return o.chunked.keyCount
	}
	return len(o.base)
}

func (o FieldIndexView) IsChunked() bool {
	return o.chunked != nil
}

func (o FieldIndexView) Rows() uint64 {
	if o.chunked != nil {
		if len(o.chunked.rowPrefix) > 0 {
			return o.chunked.rowPrefix[len(o.chunked.rowPrefix)-1]
		}
		return o.chunked.chunkRowsRange(0, o.chunked.chunkCount)
	}
	var rows uint64
	for i := range o.base {
		rows += o.base[i].IDs.Cardinality()
	}
	return rows
}

func (o FieldIndexView) RangeRows(start, end int) uint64 {
	if start < 0 || start > end || end > o.KeyCount() {
		return 0
	}
	if o.chunked != nil {
		if start == 0 && end == o.chunked.keyCount {
			if len(o.chunked.rowPrefix) > 0 {
				return o.chunked.rowPrefix[len(o.chunked.rowPrefix)-1]
			}
			return 0
		}
		return o.chunked.rangeRows(o.chunked.posForRank(start), o.chunked.posForRank(end))
	}
	var rows uint64
	for i := start; i < end; i++ {
		rows += o.base[i].IDs.Cardinality()
	}
	return rows
}

func (o FieldIndexView) RangeStats(br FieldIndexRange) (int, uint64) {
	if br.BaseStart >= br.BaseEnd {
		return 0, 0
	}
	if br.filterPrefixSet {
		cur := o.NewCursor(br, false)
		var buckets int
		var rows uint64
		for {
			ids, _, single, ok := cur.NextPostingOrSingle()
			if !ok {
				break
			}
			buckets++
			if single {
				rows++
			} else {
				rows += ids.Cardinality()
			}
		}
		return buckets, rows
	}
	if o.chunked != nil {
		start := br.startPos
		if br.BaseStart != 0 && start == (fieldIndexChunkPos{}) {
			start = o.chunked.posForRank(br.BaseStart)
		}
		end := br.endPos
		if br.BaseEnd != 0 && end == (fieldIndexChunkPos{}) {
			end = o.chunked.posForRank(br.BaseEnd)
		}
		return br.BaseEnd - br.BaseStart, o.chunked.rangeRows(start, end)
	}
	return br.BaseEnd - br.BaseStart, o.RangeRows(br.BaseStart, br.BaseEnd)
}

func (o FieldIndexView) KeyAt(rank int) keycodec.IndexKey {
	if o.chunked != nil {
		pos := o.chunked.posForRank(rank)
		ref, ok := o.chunked.refAtChunk(pos.chunk)
		if !ok {
			return keycodec.IndexKey{}
		}
		return ref.chunk.keyAt(pos.entry)
	}
	return o.base[rank].Key
}

func (o FieldIndexView) LowerBound(key string) int {
	if o.chunked != nil {
		return o.chunked.lowerBound(key)
	}
	return lowerBoundIndex(o.base, key)
}

func (o FieldIndexView) LowerBoundKey(key keycodec.IndexKey) int {
	if o.chunked != nil {
		_, rank := o.chunked.lowerBoundPosKey(key)
		return rank
	}
	return lowerBoundIndexEntriesKey(o.base, key)
}

func (o FieldIndexView) UpperBound(key string) int {
	if o.chunked != nil {
		return o.chunked.upperBound(key)
	}
	return upperBoundIndex(o.base, key)
}

func (o FieldIndexView) UpperBoundKey(key keycodec.IndexKey) int {
	if o.chunked != nil {
		_, rank := o.chunked.upperBoundPosKey(key)
		return rank
	}
	return upperBoundIndexEntriesKey(o.base, key)
}

func (o FieldIndexView) PrefixRangeEnd(prefix string, start int) int {
	if o.chunked != nil {
		return o.chunked.prefixRangeEnd(prefix, start)
	}
	return prefixRangeEndIndex(o.base, prefix, start)
}

func (o FieldIndexView) LookupCardinality(key string) uint64 {
	if o.chunked != nil {
		return o.chunked.lookupCardinality(key)
	}
	i := lowerBoundIndex(o.base, key)
	if i >= len(o.base) || !keycodec.EqualsString(o.base[i].Key, key) {
		return 0
	}
	return o.base[i].IDs.Cardinality()
}

func (o FieldIndexView) LookupCardinalityKey(key keycodec.IndexKey) uint64 {
	if o.chunked != nil {
		return o.chunked.lookupCardinalityKey(key)
	}
	i := lowerBoundIndexEntriesKey(o.base, key)
	if i >= len(o.base) || keycodec.Compare(o.base[i].Key, key) != 0 {
		return 0
	}
	return o.base[i].IDs.Cardinality()
}

func (o FieldIndexView) LookupPostingRetained(key string) posting.List {
	if o.chunked != nil {
		return o.chunked.lookupPostingRetained(key)
	}
	if len(o.base) == 0 {
		return posting.List{}
	}
	i := lowerBoundIndex(o.base, key)
	if i >= len(o.base) || !keycodec.EqualsString(o.base[i].Key, key) {
		return posting.List{}
	}
	return o.base[i].IDs.Borrow()
}

func (o FieldIndexView) LookupPostingRetainedKey(key keycodec.IndexKey) posting.List {
	if o.chunked != nil {
		return o.chunked.lookupPostingRetainedKey(key)
	}
	if len(o.base) == 0 {
		return posting.List{}
	}
	i := lowerBoundIndexEntriesKey(o.base, key)
	if i >= len(o.base) || keycodec.Compare(o.base[i].Key, key) != 0 {
		return posting.List{}
	}
	return o.base[i].IDs.Borrow()
}

func (o FieldIndexView) LookupPostings(keys []string) ([]posting.List, uint64) {
	keyCount := len(keys)
	postsBuf := posting.GetSlice(keyCount)
	var est uint64

	for i := 0; i < keyCount; i++ {
		ids := o.LookupPostingRetained(keys[i])
		if ids.IsEmpty() {
			continue
		}
		postsBuf = append(postsBuf, ids)
		est += ids.Cardinality()
	}
	return postsBuf, est
}

func (o FieldIndexView) RangeForBounds(b Bounds) FieldIndexRange {
	if o.chunked != nil {
		return o.rangeForBoundsChunked(b)
	}

	br := FieldIndexRange{
		BaseStart: 0,
		BaseEnd:   o.KeyCount(),
	}
	if b.Empty {
		br.BaseEnd = 0
		return br
	}

	if b.HasPrefix {
		ps, pe, filter := prefixRangeIndex(o.base, b.Prefix, o.LowerBound(b.Prefix))
		br.BaseStart = max(br.BaseStart, ps)
		br.BaseEnd = min(br.BaseEnd, pe)
		if filter {
			br.filterPrefix = b.Prefix
			br.filterPrefixSet = true
		}
	}

	if b.HasLo {
		bl := 0
		if b.LoNumeric {
			bl = o.LowerBoundKey(b.LoIndex)
		} else {
			bl = o.LowerBound(b.LoKey)
		}
		if !b.LoInc {
			if b.LoNumeric {
				if bl < len(o.base) && keycodec.Compare(o.base[bl].Key, b.LoIndex) == 0 {
					bl++
				}
			} else {
				if bl < len(o.base) && keycodec.EqualsString(o.base[bl].Key, b.LoKey) {
					bl++
				}
			}
		}
		br.BaseStart = max(br.BaseStart, bl)
	}

	if b.HasHi {
		var bh int
		if b.HiNumeric {
			if b.HiInc {
				bh = o.UpperBoundKey(b.HiIndex)
			} else {
				bh = o.LowerBoundKey(b.HiIndex)
			}
		} else {
			if b.HiInc {
				bh = o.UpperBound(b.HiKey)
			} else {
				bh = o.LowerBound(b.HiKey)
			}
		}
		br.BaseEnd = min(br.BaseEnd, bh)
	}

	if br.BaseStart < 0 {
		br.BaseStart = 0
	}
	if maxEnd := o.KeyCount(); br.BaseEnd > maxEnd {
		br.BaseEnd = maxEnd
	}
	if br.BaseStart > br.BaseEnd {
		br.BaseStart = br.BaseEnd
	}
	if br.filterPrefixSet {
		cur := o.NewCursor(br, false)
		if _, _, ok := cur.Next(); !ok {
			br.BaseEnd = br.BaseStart
			br.filterPrefix = ""
			br.filterPrefixSet = false
		}
	}

	return br
}

func (o FieldIndexView) rangeForBoundsChunked(b Bounds) FieldIndexRange {
	br := FieldIndexRange{
		BaseStart: 0,
		BaseEnd:   o.KeyCount(),
	}
	if o.chunked == nil {
		return br
	}
	br.startPos = fieldIndexChunkPos{}
	br.endPos = o.chunked.endPos()
	if b.Empty {
		br.BaseEnd = 0
		br.endPos = br.startPos
		return br
	}

	if b.HasPrefix {
		ps, psRank := o.chunked.lowerBoundPos(b.Prefix)
		pe, peRank, filter := o.chunked.prefixRangeEndPos(b.Prefix, &ps, &psRank)
		if psRank > br.BaseStart {
			br.BaseStart = psRank
			br.startPos = ps
		}
		if peRank < br.BaseEnd {
			br.BaseEnd = peRank
			br.endPos = pe
		}
		if filter {
			br.filterPrefix = b.Prefix
			br.filterPrefixSet = true
		}
	}

	if b.HasLo {
		var (
			bl     fieldIndexChunkPos
			blRank int
		)
		if b.LoNumeric {
			bl, blRank = o.chunked.lowerBoundPosKey(b.LoIndex)
		} else {
			bl, blRank = o.chunked.lowerBoundPos(b.LoKey)
		}
		if !b.LoInc {
			if key, ok := o.chunked.posKey(bl); ok &&
				((b.LoNumeric && keycodec.Compare(key, b.LoIndex) == 0) ||
					(!b.LoNumeric && keycodec.EqualsString(key, b.LoKey))) {
				bl = o.chunked.advancePos(bl)
				blRank++
			}
		}
		if blRank > br.BaseStart {
			br.BaseStart = blRank
			br.startPos = bl
		}
	}

	if b.HasHi {
		var (
			bh     fieldIndexChunkPos
			bhRank int
		)
		if b.HiNumeric {
			if b.HiInc {
				bh, bhRank = o.chunked.upperBoundPosKey(b.HiIndex)
			} else {
				bh, bhRank = o.chunked.lowerBoundPosKey(b.HiIndex)
			}
		} else {
			if b.HiInc {
				bh, bhRank = o.chunked.upperBoundPos(b.HiKey)
			} else {
				bh, bhRank = o.chunked.lowerBoundPos(b.HiKey)
			}
		}
		if bhRank < br.BaseEnd {
			br.BaseEnd = bhRank
			br.endPos = bh
		}
	}

	if br.BaseStart < 0 {
		br.BaseStart = 0
		br.startPos = fieldIndexChunkPos{}
	}
	if maxEnd := o.KeyCount(); br.BaseEnd > maxEnd {
		br.BaseEnd = maxEnd
		br.endPos = o.chunked.endPos()
	}
	if br.BaseStart > br.BaseEnd {
		br.BaseStart = br.BaseEnd
		br.startPos = br.endPos
	}
	if br.filterPrefixSet {
		cur := o.NewCursor(br, false)
		if _, _, ok := cur.Next(); !ok {
			br.BaseEnd = br.BaseStart
			br.endPos = br.startPos
			br.filterPrefix = ""
			br.filterPrefixSet = false
		}
	}
	return br
}

func (o FieldIndexView) RangeByRanks(start, end int) FieldIndexRange {
	if start < 0 {
		start = 0
	}
	if maxEnd := o.KeyCount(); end > maxEnd {
		end = maxEnd
	}
	if start > end {
		start = end
	}
	br := FieldIndexRange{
		BaseStart: start,
		BaseEnd:   end,
	}
	if o.chunked != nil {
		br.startPos = o.chunked.posForRank(start)
		br.endPos = o.chunked.posForRank(end)
	}
	return br
}

func (o FieldIndexView) NewCursor(br FieldIndexRange, desc bool) FieldIndexCursor {
	c := FieldIndexCursor{
		base:    o.base,
		chunked: o.chunked,
		desc:    desc,

		filterPrefix:    br.filterPrefix,
		filterPrefixSet: br.filterPrefixSet,
	}
	if o.chunked != nil {
		c.remaining = br.BaseEnd - br.BaseStart
		if c.remaining <= 0 {
			return c
		}
		if desc {
			pos, ok := o.chunked.prevPos(br.endPos)
			if !ok {
				c.remaining = 0
				return c
			}
			c.chunkIdx = pos.chunk
			c.entryIdx = pos.entry
			c.pageIdx, c.refIdx = o.chunked.pagePosForChunk(pos.chunk)
			if c.pageIdx >= len(o.chunked.pages) {
				c.remaining = 0
				return c
			}
			c.chunk = o.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
			return c
		}
		c.chunkIdx = br.startPos.chunk
		c.entryIdx = br.startPos.entry
		c.pageIdx, c.refIdx = o.chunked.pagePosForChunk(br.startPos.chunk)
		if c.pageIdx >= len(o.chunked.pages) {
			c.remaining = 0
			return c
		}
		c.chunk = o.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
		return c
	}
	if desc {
		c.pos = br.BaseEnd - 1
		c.end = br.BaseStart
		return c
	}
	c.pos = br.BaseStart
	c.end = br.BaseEnd
	return c
}

func (c *FieldIndexCursor) Next() (keycodec.IndexKey, posting.List, bool) {
	if c.filterPrefixSet {
		key, ids, idx, single, ok := c.NextKeyPostingOrSingle()
		if !ok {
			return keycodec.IndexKey{}, posting.List{}, false
		}
		if single {
			var p posting.List
			return key, p.BuildAdded(idx), true
		}
		return key, ids, true
	}
	if c.desc {
		if c.chunked != nil {
			if c.remaining <= 0 || c.chunk == nil {
				return keycodec.IndexKey{}, posting.List{}, false
			}
			key := c.chunk.keyAt(c.entryIdx)
			ids := c.chunk.postingAt(c.entryIdx)
			c.remaining--
			if c.remaining > 0 {
				if c.entryIdx > 0 {
					c.entryIdx--
				} else {
					c.chunkIdx--
					if c.refIdx > 0 {
						c.refIdx--
					} else {
						c.pageIdx--
						if c.pageIdx < 0 {
							c.chunk = nil
							return keycodec.IndexKey{}, posting.List{}, false
						}
						c.refIdx = len(c.chunked.pages[c.pageIdx].refs) - 1
					}
					c.chunk = c.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
					c.entryIdx = c.chunk.keyCount() - 1
				}
			}
			return key, ids, true
		}
		if c.pos < c.end {
			return keycodec.IndexKey{}, posting.List{}, false
		}
		ent := c.base[c.pos]
		c.pos--
		return ent.Key, ent.IDs.Borrow(), true
	}
	if c.chunked != nil {
		if c.remaining <= 0 || c.chunk == nil {
			return keycodec.IndexKey{}, posting.List{}, false
		}
		key := c.chunk.keyAt(c.entryIdx)
		ids := c.chunk.postingAt(c.entryIdx)
		c.remaining--
		if c.remaining > 0 {
			c.entryIdx++
			if c.entryIdx >= c.chunk.keyCount() {
				c.chunkIdx++
				c.refIdx++
				if c.refIdx >= len(c.chunked.pages[c.pageIdx].refs) {
					c.pageIdx++
					if c.pageIdx >= len(c.chunked.pages) {
						c.chunk = nil
						return keycodec.IndexKey{}, posting.List{}, false
					}
					c.refIdx = 0
				}
				c.chunk = c.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
				c.entryIdx = 0
			}
		}
		return key, ids, true
	}
	if c.pos >= c.end {
		return keycodec.IndexKey{}, posting.List{}, false
	}
	ent := c.base[c.pos]
	c.pos++
	return ent.Key, ent.IDs.Borrow(), true
}

func (c *FieldIndexCursor) NextPostingOrSingle() (posting.List, uint64, bool, bool) {
	if c.filterPrefixSet {
		_, ids, idx, single, ok := c.NextKeyPostingOrSingle()
		return ids, idx, single, ok
	}
	if c.desc {
		if c.chunked != nil {
			if c.remaining <= 0 || c.chunk == nil {
				return posting.List{}, 0, false, false
			}
			single := c.chunk.posts == nil
			var (
				ids posting.List
				idx uint64
			)
			if single {
				if c.chunk.stringRefs != nil {
					idx = c.chunk.numeric[c.entryIdx]
				} else {
					idx = c.chunk.numeric[(c.entryIdx<<1)+1]
				}
			} else {
				ids = c.chunk.posts[c.entryIdx].Borrow()
			}
			c.remaining--
			if c.remaining > 0 {
				if c.entryIdx > 0 {
					c.entryIdx--
				} else {
					c.chunkIdx--
					if c.refIdx > 0 {
						c.refIdx--
					} else {
						c.pageIdx--
						if c.pageIdx < 0 {
							c.chunk = nil
							if single {
								return posting.List{}, idx, true, true
							}
							if singleIdx, ok := ids.TrySingle(); ok {
								return posting.List{}, singleIdx, true, true
							}
							return ids, 0, false, true
						}
						c.refIdx = len(c.chunked.pages[c.pageIdx].refs) - 1
					}
					c.chunk = c.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
					c.entryIdx = c.chunk.keyCount() - 1
				}
			}
			if single {
				return posting.List{}, idx, true, true
			}
			if singleIdx, ok := ids.TrySingle(); ok {
				return posting.List{}, singleIdx, true, true
			}
			return ids, 0, false, true
		}
		if c.pos < c.end {
			return posting.List{}, 0, false, false
		}
		ent := c.base[c.pos]
		c.pos--
		if idx, ok := ent.IDs.TrySingle(); ok {
			return posting.List{}, idx, true, true
		}
		return ent.IDs.Borrow(), 0, false, true
	}
	if c.chunked != nil {
		if c.remaining <= 0 || c.chunk == nil {
			return posting.List{}, 0, false, false
		}
		single := c.chunk.posts == nil
		var (
			ids posting.List
			idx uint64
		)
		if single {
			if c.chunk.stringRefs != nil {
				idx = c.chunk.numeric[c.entryIdx]
			} else {
				idx = c.chunk.numeric[(c.entryIdx<<1)+1]
			}
		} else {
			ids = c.chunk.posts[c.entryIdx].Borrow()
		}
		c.remaining--
		if c.remaining > 0 {
			c.entryIdx++
			if c.entryIdx >= c.chunk.keyCount() {
				c.chunkIdx++
				c.refIdx++
				if c.refIdx >= len(c.chunked.pages[c.pageIdx].refs) {
					c.pageIdx++
					if c.pageIdx >= len(c.chunked.pages) {
						c.chunk = nil
						if single {
							return posting.List{}, idx, true, true
						}
						if singleIdx, ok := ids.TrySingle(); ok {
							return posting.List{}, singleIdx, true, true
						}
						return ids, 0, false, true
					}
					c.refIdx = 0
				}
				c.chunk = c.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
				c.entryIdx = 0
			}
		}
		if single {
			return posting.List{}, idx, true, true
		}
		if singleIdx, ok := ids.TrySingle(); ok {
			return posting.List{}, singleIdx, true, true
		}
		return ids, 0, false, true
	}
	if c.pos >= c.end {
		return posting.List{}, 0, false, false
	}
	ent := c.base[c.pos]
	c.pos++
	if idx, ok := ent.IDs.TrySingle(); ok {
		return posting.List{}, idx, true, true
	}
	return ent.IDs.Borrow(), 0, false, true
}

func (c *FieldIndexCursor) NextKeyPostingOrSingle() (keycodec.IndexKey, posting.List, uint64, bool, bool) {
	if !c.filterPrefixSet {
		return c.nextKeyPostingOrSingle()
	}
	if c.chunked != nil {
		for c.remaining > 0 && c.chunk != nil {
			if c.chunk.hasStringKeys() {
				return c.nextKeyPostingOrSingle()
			}
			if c.desc {
				n := c.entryIdx + 1
				if n > c.remaining {
					n = c.remaining
				}
				c.remaining -= n
				if c.remaining <= 0 {
					break
				}
				c.chunkIdx--
				if c.refIdx > 0 {
					c.refIdx--
				} else {
					c.pageIdx--
					if c.pageIdx < 0 {
						c.chunk = nil
						break
					}
					c.refIdx = len(c.chunked.pages[c.pageIdx].refs) - 1
				}
				c.chunk = c.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
				c.entryIdx = c.chunk.keyCount() - 1
				continue
			}
			n := c.chunk.keyCount() - c.entryIdx
			if n > c.remaining {
				n = c.remaining
			}
			c.remaining -= n
			if c.remaining <= 0 {
				break
			}
			c.chunkIdx++
			c.refIdx++
			if c.refIdx >= len(c.chunked.pages[c.pageIdx].refs) {
				c.pageIdx++
				if c.pageIdx >= len(c.chunked.pages) {
					c.chunk = nil
					break
				}
				c.refIdx = 0
			}
			c.chunk = c.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
			c.entryIdx = 0
		}
		return keycodec.IndexKey{}, posting.List{}, 0, false, false
	}
	for {
		key, ids, idx, single, ok := c.nextKeyPostingOrSingle()
		if !ok || keycodec.HasPrefixString(key, c.filterPrefix) {
			return key, ids, idx, single, ok
		}
	}
}

func (c *FieldIndexCursor) nextKeyPostingOrSingle() (keycodec.IndexKey, posting.List, uint64, bool, bool) {
	if c.desc {
		if c.chunked != nil {
			if c.remaining <= 0 || c.chunk == nil {
				return keycodec.IndexKey{}, posting.List{}, 0, false, false
			}
			key := c.chunk.keyAt(c.entryIdx)
			single := c.chunk.posts == nil
			var (
				ids posting.List
				idx uint64
			)
			if single {
				if c.chunk.stringRefs != nil {
					idx = c.chunk.numeric[c.entryIdx]
				} else {
					idx = c.chunk.numeric[(c.entryIdx<<1)+1]
				}
			} else {
				ids = c.chunk.posts[c.entryIdx].Borrow()
			}
			c.remaining--
			if c.remaining > 0 {
				if c.entryIdx > 0 {
					c.entryIdx--
				} else {
					c.chunkIdx--
					if c.refIdx > 0 {
						c.refIdx--
					} else {
						c.pageIdx--
						if c.pageIdx < 0 {
							c.chunk = nil
							if single {
								return key, posting.List{}, idx, true, true
							}
							if singleIdx, ok := ids.TrySingle(); ok {
								return key, posting.List{}, singleIdx, true, true
							}
							return key, ids, 0, false, true
						}
						c.refIdx = len(c.chunked.pages[c.pageIdx].refs) - 1
					}
					c.chunk = c.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
					c.entryIdx = c.chunk.keyCount() - 1
				}
			}
			if single {
				return key, posting.List{}, idx, true, true
			}
			if singleIdx, ok := ids.TrySingle(); ok {
				return key, posting.List{}, singleIdx, true, true
			}
			return key, ids, 0, false, true
		}
		if c.pos < c.end {
			return keycodec.IndexKey{}, posting.List{}, 0, false, false
		}
		ent := c.base[c.pos]
		c.pos--

		if idx, ok := ent.IDs.TrySingle(); ok {
			return ent.Key, posting.List{}, idx, true, true
		}
		return ent.Key, ent.IDs.Borrow(), 0, false, true
	}

	if c.chunked != nil {
		if c.remaining <= 0 || c.chunk == nil {
			return keycodec.IndexKey{}, posting.List{}, 0, false, false
		}
		key := c.chunk.keyAt(c.entryIdx)
		single := c.chunk.posts == nil
		var (
			ids posting.List
			idx uint64
		)
		if single {
			if c.chunk.stringRefs != nil {
				idx = c.chunk.numeric[c.entryIdx]
			} else {
				idx = c.chunk.numeric[(c.entryIdx<<1)+1]
			}
		} else {
			ids = c.chunk.posts[c.entryIdx].Borrow()
		}
		c.remaining--
		if c.remaining > 0 {
			c.entryIdx++
			if c.entryIdx >= c.chunk.keyCount() {
				c.chunkIdx++
				c.refIdx++
				if c.refIdx >= len(c.chunked.pages[c.pageIdx].refs) {
					c.pageIdx++
					if c.pageIdx >= len(c.chunked.pages) {
						c.chunk = nil
						if single {
							return key, posting.List{}, idx, true, true
						}
						if singleIdx, ok := ids.TrySingle(); ok {
							return key, posting.List{}, singleIdx, true, true
						}
						return key, ids, 0, false, true
					}
					c.refIdx = 0
				}
				c.chunk = c.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
				c.entryIdx = 0
			}
		}
		if single {
			return key, posting.List{}, idx, true, true
		}
		if singleIdx, ok := ids.TrySingle(); ok {
			return key, posting.List{}, singleIdx, true, true
		}
		return key, ids, 0, false, true
	}
	if c.pos >= c.end {
		return keycodec.IndexKey{}, posting.List{}, 0, false, false
	}
	ent := c.base[c.pos]
	c.pos++

	if idx, ok := ent.IDs.TrySingle(); ok {
		return ent.Key, posting.List{}, idx, true, true
	}
	return ent.Key, ent.IDs.Borrow(), 0, false, true
}

func (o FieldIndexView) AppendPostingFilter(out []uint64, br FieldIndexRange, desc bool, filter posting.List, offset, limit uint64) ([]uint64, uint64, bool) {
	if filter.IsEmpty() || br.Empty() || limit == 0 {
		return out, 0, true
	}

	need := limit
	skip := offset
	var examined uint64
	var filterCur posting.ContainsCursor
	filterCur.Reset(filter)

	if br.filterPrefixSet {
		cur := o.NewCursor(br, desc)
		for {
			ids, idx, single, ok := cur.NextPostingOrSingle()
			if !ok {
				return out, examined, true
			}
			if single {
				examined++
				if filterCur.Contains(idx) {
					if skip > 0 {
						skip--
					} else {
						out = append(out, idx)
						need--
						if need == 0 {
							return out, examined, true
						}
					}
				}
				continue
			}
			it := ids.Iter()
			for it.HasNext() {
				idx := it.Next()
				examined++
				if !filterCur.Contains(idx) {
					continue
				}
				if skip > 0 {
					skip--
					continue
				}
				out = append(out, idx)
				need--
				if need == 0 {
					it.Release()
					return out, examined, true
				}
			}
			it.Release()
		}
	}

	if o.chunked == nil {
		if desc {
			for i := br.BaseEnd - 1; i >= br.BaseStart; i-- {
				ids := o.base[i].IDs.Borrow()
				if idx, ok := ids.TrySingle(); ok {
					examined++
					if filterCur.Contains(idx) {
						if skip > 0 {
							skip--
						} else {
							out = append(out, idx)
							need--
							if need == 0 {
								return out, examined, true
							}
						}
					}
					if i == br.BaseStart {
						break
					}
					continue
				}
				it := ids.Iter()
				for it.HasNext() {
					idx := it.Next()
					examined++
					if !filterCur.Contains(idx) {
						continue
					}
					if skip > 0 {
						skip--
						continue
					}
					out = append(out, idx)
					need--
					if need == 0 {
						it.Release()
						return out, examined, true
					}
				}
				it.Release()
				if i == br.BaseStart {
					break
				}
			}
			return out, examined, true
		}
		for i := br.BaseStart; i < br.BaseEnd; i++ {
			ids := o.base[i].IDs.Borrow()
			if idx, ok := ids.TrySingle(); ok {
				examined++
				if filterCur.Contains(idx) {
					if skip > 0 {
						skip--
					} else {
						out = append(out, idx)
						need--
						if need == 0 {
							return out, examined, true
						}
					}
				}
				continue
			}
			it := ids.Iter()
			for it.HasNext() {
				idx := it.Next()
				examined++
				if !filterCur.Contains(idx) {
					continue
				}
				if skip > 0 {
					skip--
					continue
				}
				out = append(out, idx)
				need--
				if need == 0 {
					it.Release()
					return out, examined, true
				}
			}
			it.Release()
		}
		return out, examined, true
	}

	c := o.NewCursor(br, desc)
	if desc {
		for c.remaining > 0 && c.chunk != nil {
			chunk := c.chunk
			n := c.entryIdx + 1
			if n > c.remaining {
				n = c.remaining
			}
			end := c.entryIdx - n + 1
			if chunk.posts == nil {
				for i := c.entryIdx; ; i-- {
					var idx uint64
					if chunk.stringRefs != nil {
						idx = chunk.numeric[i]
					} else {
						idx = chunk.numeric[(i<<1)+1]
					}
					examined++
					if filterCur.Contains(idx) {
						if skip > 0 {
							skip--
						} else {
							out = append(out, idx)
							need--
							if need == 0 {
								return out, examined, true
							}
						}
					}
					if i == end {
						break
					}
				}
			} else {
				for i := c.entryIdx; ; i-- {
					ids := chunk.posts[i].Borrow()
					it := ids.Iter()
					for it.HasNext() {
						idx := it.Next()
						examined++
						if !filterCur.Contains(idx) {
							continue
						}
						if skip > 0 {
							skip--
							continue
						}
						out = append(out, idx)
						need--
						if need == 0 {
							it.Release()
							return out, examined, true
						}
					}
					it.Release()
					if i == end {
						break
					}
				}
			}
			c.remaining -= n
			if c.remaining <= 0 {
				break
			}
			c.chunkIdx--
			if c.refIdx > 0 {
				c.refIdx--
			} else {
				c.pageIdx--
				if c.pageIdx < 0 {
					break
				}
				c.refIdx = len(c.chunked.pages[c.pageIdx].refs) - 1
			}
			c.chunk = c.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
			c.entryIdx = c.chunk.keyCount() - 1
		}
		return out, examined, true
	}

	for c.remaining > 0 && c.chunk != nil {
		chunk := c.chunk
		n := chunk.keyCount() - c.entryIdx
		if n > c.remaining {
			n = c.remaining
		}
		end := c.entryIdx + n
		if chunk.posts == nil {
			for i := c.entryIdx; i < end; i++ {
				var idx uint64
				if chunk.stringRefs != nil {
					idx = chunk.numeric[i]
				} else {
					idx = chunk.numeric[(i<<1)+1]
				}
				examined++
				if !filterCur.Contains(idx) {
					continue
				}
				if skip > 0 {
					skip--
					continue
				}
				out = append(out, idx)
				need--
				if need == 0 {
					return out, examined, true
				}
			}
		} else {
			for i := c.entryIdx; i < end; i++ {
				ids := chunk.posts[i].Borrow()
				it := ids.Iter()
				for it.HasNext() {
					idx := it.Next()
					examined++
					if !filterCur.Contains(idx) {
						continue
					}
					if skip > 0 {
						skip--
						continue
					}
					out = append(out, idx)
					need--
					if need == 0 {
						it.Release()
						return out, examined, true
					}
				}
				it.Release()
			}
		}
		c.remaining -= n
		if c.remaining <= 0 {
			break
		}
		c.chunkIdx++
		c.refIdx++
		if c.refIdx >= len(c.chunked.pages[c.pageIdx].refs) {
			c.pageIdx++
			if c.pageIdx >= len(c.chunked.pages) {
				break
			}
			c.refIdx = 0
		}
		c.chunk = c.chunked.pages[c.pageIdx].refs[c.refIdx].chunk
		c.entryIdx = 0
	}
	return out, examined, true
}

func lowerBoundIndex(s []Entry, key string) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi) >> 1
		if keycodec.CompareString(s[mid].Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func upperBoundIndex(s []Entry, key string) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi) >> 1
		if keycodec.CompareString(s[mid].Key, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func prefixRangeEndIndex(s []Entry, prefix string, start int) int {
	_, end, _ := prefixRangeIndex(s, prefix, start)
	return end
}

func prefixRangeIndex(s []Entry, prefix string, start int) (int, int, bool) {
	if start < 0 || start >= len(s) {
		return start, start, false
	}
	end := len(s)
	upper, ok := keycodec.NewPrefixUpperBound(prefix)
	if ok {
		lo, hi := start, len(s)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if keycodec.ComparePrefixUpperBound(s[mid].Key, upper) >= 0 {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		end = lo
	}
	if prefix == "" {
		return start, end, false
	}
	for start < end && !keycodec.HasPrefixString(s[start].Key, prefix) {
		start++
	}
	if start >= end {
		return start, start, false
	}
	for i := start + 1; i < end; i++ {
		if !keycodec.HasPrefixString(s[i].Key, prefix) {
			return start, end, true
		}
	}
	return start, end, false
}
