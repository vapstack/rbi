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
		cmp := strings.Compare(b.HiKey, b.Prefix)
		if b.HiNumeric {
			cmp = keycodec.CompareString(b.HiIndex, b.Prefix)
		}
		if cmp < 0 || (cmp == 0 && !b.HiInc) {
			b.SetEmpty()
			return
		}
	}

	if b.HasLo {
		if upper, ok := keycodec.NewPrefixUpperBound(b.Prefix); ok {
			cmp := keycodec.CompareStringPrefixUpperBound(b.LoKey, upper)
			if b.LoNumeric {
				cmp = keycodec.ComparePrefixUpperBound(b.LoIndex, upper)
			}
			if cmp >= 0 {
				b.SetEmpty()
			}
		}
	}
}

func (b Bounds) IsPointRange() bool {
	return !b.HasPrefix && b.HasLo && b.HasHi && b.LoInc && b.HiInc && b.compareLoHi() == 0
}

func (b Bounds) compareLoHi() int {
	if b.LoNumeric != b.HiNumeric {
		panic("rbi: mixed range bound key representations")
	}
	if b.LoNumeric {
		return keycodec.Compare(b.LoIndex, b.HiIndex)
	}
	return strings.Compare(b.LoKey, b.HiKey)
}

func (b *Bounds) ApplyLo(key string, inc bool) {
	if b.Empty {
		return
	}
	if !b.HasLo || b.LoKey < key || (b.LoKey == key && b.LoInc && !inc) {
		b.HasLo = true
		b.LoKey = key
		b.LoIndex = keycodec.IndexKey{}
		b.LoNumeric = false
		b.LoInc = inc
	}
	b.Normalize()
}

func (b *Bounds) ApplyLoIndex(key keycodec.IndexKey, inc bool) {
	if b.Empty {
		return
	}
	if !b.HasLo || keycodec.Compare(b.LoIndex, key) < 0 || (keycodec.Compare(b.LoIndex, key) == 0 && b.LoInc && !inc) {
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
	if !b.HasHi || b.HiKey > key || (b.HiKey == key && b.HiInc && !inc) {
		b.HasHi = true
		b.HiKey = key
		b.HiIndex = keycodec.IndexKey{}
		b.HiNumeric = false
		b.HiInc = inc
	}
	b.Normalize()
}

func (b *Bounds) ApplyHiIndex(key keycodec.IndexKey, inc bool) {
	if b.Empty {
		return
	}
	if !b.HasHi || keycodec.Compare(b.HiIndex, key) > 0 || (keycodec.Compare(b.HiIndex, key) == 0 && b.HiInc && !inc) {
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

// FieldOverlay provides read helpers over one immutable field storage view.
type FieldOverlay struct {
	base    []Entry
	chunked *fieldIndexChunkedRoot
}

type OverlayRange struct {
	BaseStart int
	BaseEnd   int

	startPos fieldIndexChunkPos
	endPos   fieldIndexChunkPos
}

type OverlayCursor struct {
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
}

func (br OverlayRange) Empty() bool {
	return br.BaseStart >= br.BaseEnd
}

func (br OverlayRange) Len() int {
	if br.BaseStart >= br.BaseEnd {
		return 0
	}
	return br.BaseEnd - br.BaseStart
}

func NewFieldOverlay(base *[]Entry) FieldOverlay {
	if base == nil {
		return FieldOverlay{}
	}
	return FieldOverlay{base: *base}
}

func NewFieldOverlayStorage(storage FieldStorage) FieldOverlay {
	if storage.chunked != nil {
		return FieldOverlay{chunked: storage.chunked}
	}
	if storage.flat != nil {
		return FieldOverlay{base: storage.flat.entries}
	}
	return FieldOverlay{}
}

func (o FieldOverlay) HasData() bool {
	return o.KeyCount() > 0
}

func (o FieldOverlay) KeyCount() int {
	if o.chunked != nil {
		return o.chunked.keyCount
	}
	return len(o.base)
}

func (o FieldOverlay) IsChunked() bool {
	return o.chunked != nil
}

func (o FieldOverlay) Rows() uint64 {
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

func (o FieldOverlay) RangeRows(start, end int) uint64 {
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

func (o FieldOverlay) RangeStats(br OverlayRange) (int, uint64) {
	if br.BaseStart >= br.BaseEnd {
		return 0, 0
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

func (o FieldOverlay) KeyAt(rank int) keycodec.IndexKey {
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

func (o FieldOverlay) LowerBound(key string) int {
	if o.chunked != nil {
		return o.chunked.lowerBound(key)
	}
	return lowerBoundIndex(o.base, key)
}

func (o FieldOverlay) LowerBoundKey(key keycodec.IndexKey) int {
	if o.chunked != nil {
		_, rank := o.chunked.lowerBoundPosKey(key)
		return rank
	}
	return lowerBoundIndexEntriesKey(o.base, key)
}

func (o FieldOverlay) UpperBound(key string) int {
	if o.chunked != nil {
		return o.chunked.upperBound(key)
	}
	return upperBoundIndex(o.base, key)
}

func (o FieldOverlay) UpperBoundKey(key keycodec.IndexKey) int {
	if o.chunked != nil {
		_, rank := o.chunked.upperBoundPosKey(key)
		return rank
	}
	return upperBoundIndexEntriesKey(o.base, key)
}

func (o FieldOverlay) PrefixRangeEnd(prefix string, start int) int {
	if o.chunked != nil {
		return o.chunked.prefixRangeEnd(prefix, start)
	}
	return prefixRangeEndIndex(o.base, prefix, start)
}

func (o FieldOverlay) LookupCardinality(key string) uint64 {
	if o.chunked != nil {
		return o.chunked.lookupCardinality(key)
	}
	i := lowerBoundIndex(o.base, key)
	if i >= len(o.base) || !keycodec.EqualsString(o.base[i].Key, key) {
		return 0
	}
	return o.base[i].IDs.Cardinality()
}

func (o FieldOverlay) LookupPostingRetained(key string) posting.List {
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

func (o FieldOverlay) PostingAt(rank int) posting.List {
	if rank < 0 || rank >= o.KeyCount() {
		return posting.List{}
	}
	if o.chunked != nil {
		pos := o.chunked.posForRank(rank)
		ref, ok := o.chunked.refAtChunk(pos.chunk)
		if !ok {
			return posting.List{}
		}
		return ref.chunk.postingAt(pos.entry)
	}
	return o.base[rank].IDs.Borrow()
}

func (o FieldOverlay) LookupPostings(keys []string) ([]posting.List, uint64) {
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

func (o FieldOverlay) RangeForBounds(b Bounds) OverlayRange {
	if o.chunked != nil {
		return o.rangeForBoundsChunked(b)
	}

	br := OverlayRange{
		BaseStart: 0,
		BaseEnd:   o.KeyCount(),
	}
	if b.Empty {
		br.BaseEnd = 0
		return br
	}

	if b.HasPrefix {
		ps := o.LowerBound(b.Prefix)
		pe := o.PrefixRangeEnd(b.Prefix, ps)
		br.BaseStart = max(br.BaseStart, ps)
		br.BaseEnd = min(br.BaseEnd, pe)
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

	return br
}

func (o FieldOverlay) rangeForBoundsChunked(b Bounds) OverlayRange {
	br := OverlayRange{
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
		pe, peRank := o.chunked.prefixRangeEndPos(b.Prefix, ps, psRank)
		if psRank > br.BaseStart {
			br.BaseStart = psRank
			br.startPos = ps
		}
		if peRank < br.BaseEnd {
			br.BaseEnd = peRank
			br.endPos = pe
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
	return br
}

func (o FieldOverlay) RangeByRanks(start, end int) OverlayRange {
	if start < 0 {
		start = 0
	}
	if maxEnd := o.KeyCount(); end > maxEnd {
		end = maxEnd
	}
	if start > end {
		start = end
	}
	br := OverlayRange{
		BaseStart: start,
		BaseEnd:   end,
	}
	if o.chunked != nil {
		br.startPos = o.chunked.posForRank(start)
		br.endPos = o.chunked.posForRank(end)
	}
	return br
}

func (o FieldOverlay) NewCursor(br OverlayRange, desc bool) OverlayCursor {
	c := OverlayCursor{
		base:    o.base,
		chunked: o.chunked,
		desc:    desc,
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

func (c *OverlayCursor) Next() (keycodec.IndexKey, posting.List, bool) {
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
	if start < 0 || start >= len(s) {
		return start
	}
	if keycodec.CompareString(s[start].Key, prefix) < 0 || !keycodec.HasPrefixString(s[start].Key, prefix) {
		return start
	}
	upper, ok := keycodec.NewPrefixUpperBound(prefix)
	if !ok {
		return len(s)
	}

	lo, hi := start, len(s)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if keycodec.ComparePrefixUpperBound(s[mid].Key, upper) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}
