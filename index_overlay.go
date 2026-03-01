package rbi

import (
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// indexDeltaEntry stores per-key delta changes relative to immutable base.
// add contributes ids; del removes ids.
type indexDeltaEntry struct {
	add *roaring64.Bitmap
	del *roaring64.Bitmap

	addSingle    uint64
	delSingle    uint64
	addSingleSet bool
	delSingleSet bool
}

// fieldIndexDelta is a compact immutable key overlay for one field.
// keys must be sorted lexicographically.
type fieldIndexDelta struct {
	keys  []string
	byKey map[string]indexDeltaEntry
	// Singleton delta entries are stored inline to avoid map overhead
	// when only one key is present in the field delta.
	singleKey   string
	singleEntry indexDeltaEntry
	singleSet   bool
	fixed8      bool
	ops         uint64

	keysOnce sync.Once
}

// fieldOverlay provides read helpers over base sorted index + optional delta
type fieldOverlay struct {
	base   []index
	delta  *fieldIndexDelta
	fixed8 bool
}

type overlayRange struct {
	baseStart  int
	baseEnd    int
	deltaStart int
	deltaEnd   int
}

type overlayKeyCursor struct {
	base  []index
	delta *fieldIndexDelta
	keys  []string

	b, be int
	d, de int

	desc   bool
	fixed8 bool
}

func newFieldOverlay(base *[]index, delta *fieldIndexDelta) fieldOverlay {
	if base == nil {
		return fieldOverlay{
			delta:  delta,
			fixed8: delta != nil && delta.fixed8,
		}
	}
	baseSlice := *base
	fixed8 := delta != nil && delta.fixed8
	if len(baseSlice) > 0 {
		if baseSlice[0].Key.isNumeric() || baseSlice[len(baseSlice)-1].Key.isNumeric() {
			fixed8 = true
		}
	}
	return fieldOverlay{
		base:   baseSlice,
		delta:  delta,
		fixed8: fixed8,
	}
}

func (o fieldOverlay) hasData() bool {
	if len(o.base) > 0 {
		return true
	}
	return o.delta != nil && o.delta.hasEntries()
}

func (o fieldOverlay) lookup(key string, scratch *roaring64.Bitmap) *roaring64.Bitmap {
	bm, _ := o.lookupWithState(key, scratch)
	return bm
}

func (o fieldOverlay) lookupWithState(key string, scratch *roaring64.Bitmap) (*roaring64.Bitmap, bool) {
	var base postingList
	if len(o.base) > 0 {
		if i := lowerBoundIndex(o.base, key); i < len(o.base) && indexKeyEqualsString(o.base[i].Key, key) {
			base = o.base[i].IDs
		}
	}

	if o.delta == nil || !o.delta.hasEntries() {
		return base.ToBitmapOwned(scratch)
	}
	de, ok := o.delta.get(key)
	if !ok {
		return base.ToBitmapOwned(scratch)
	}
	return composePostingOwned(base, de, scratch)
}

func composePosting(base postingList, de indexDeltaEntry, scratch *roaring64.Bitmap) *roaring64.Bitmap {
	bm, _ := composePostingOwned(base, de, scratch)
	return bm
}

func composePostingOwned(base postingList, de indexDeltaEntry, scratch *roaring64.Bitmap) (*roaring64.Bitmap, bool) {
	if deltaEntryIsEmpty(de) {
		return base.ToBitmapOwned(scratch)
	}

	// empty base (new key materialized from delta only)
	if base.IsEmpty() {
		if !deltaEntryHasAdd(de) {
			return nil, false
		}
		if de.add != nil && !de.add.IsEmpty() && !deltaEntryHasDel(de) {
			return de.add, false
		}
		s := resetOrNewBitmap(scratch)
		deltaEntryApplyAddToBitmap(s, de)
		deltaEntryApplyDelToBitmap(s, de)
		if s.IsEmpty() {
			return nil, true
		}
		return s, true
	}

	// singleton base can often stay singleton without bitmap copy.
	if base.isSingleton() {
		baseID := base.single
		needsCopy := false

		if deltaEntryHasAdd(de) {
			if de.addSingleSet && de.addSingle != baseID {
				needsCopy = true
			}
			if !needsCopy && de.add != nil && !de.add.IsEmpty() {
				if de.add.GetCardinality() != 1 || !de.add.Contains(baseID) {
					needsCopy = true
				}
			}
		}
		if deltaEntryHasDel(de) {
			if de.delSingleSet && de.delSingle == baseID {
				needsCopy = true
			}
			if !needsCopy && de.del != nil && !de.del.IsEmpty() && de.del.Contains(baseID) {
				needsCopy = true
			}
		}

		if !needsCopy {
			return base.ToBitmapOwned(scratch)
		}

		s := resetOrNewBitmap(scratch)
		s.Add(baseID)
		deltaEntryApplyAddToBitmap(s, de)
		deltaEntryApplyDelToBitmap(s, de)
		if s.IsEmpty() {
			return nil, true
		}
		return s, true
	}

	// bitmap-backed base
	baseBM := base.bitmap()
	if deltaEntryHasAdd(de) {
		if de.addSingleSet && !baseBM.Contains(de.addSingle) {
			s := resetOrNewBitmap(scratch)
			s.Or(baseBM)
			deltaEntryApplyAddToBitmap(s, de)
			deltaEntryApplyDelToBitmap(s, de)
			if s.IsEmpty() {
				return nil, true
			}
			return s, true
		}
		if de.add != nil && !de.add.IsEmpty() {
			s := resetOrNewBitmap(scratch)
			s.Or(baseBM)
			deltaEntryApplyAddToBitmap(s, de)
			deltaEntryApplyDelToBitmap(s, de)
			if s.IsEmpty() {
				return nil, true
			}
			return s, true
		}
	}
	if deltaEntryHasDel(de) {
		if de.delSingleSet && baseBM.Contains(de.delSingle) {
			s := resetOrNewBitmap(scratch)
			s.Or(baseBM)
			deltaEntryApplyAddToBitmap(s, de)
			deltaEntryApplyDelToBitmap(s, de)
			if s.IsEmpty() {
				return nil, true
			}
			return s, true
		}
		if de.del != nil && !de.del.IsEmpty() && baseBM.Intersects(de.del) {
			s := resetOrNewBitmap(scratch)
			s.Or(baseBM)
			deltaEntryApplyAddToBitmap(s, de)
			deltaEntryApplyDelToBitmap(s, de)
			if s.IsEmpty() {
				return nil, true
			}
			return s, true
		}
	}
	return baseBM, false
}

// composePostingCardinality returns exact cardinality of effective posting
// ((base ∪ add) \ del) without materializing composed bitmap in common cases.
// It falls back to composePostingOwned only when add/del overlap is detected.
func composePostingCardinality(base postingList, de indexDeltaEntry) uint64 {
	if deltaEntryIsEmpty(de) {
		return base.Cardinality()
	}

	baseCard := base.Cardinality()
	if baseCard == 0 {
		if de.addSingleSet {
			if de.delSingleSet && de.delSingle == de.addSingle {
				return 0
			}
			if de.del != nil && !de.del.IsEmpty() && de.del.Contains(de.addSingle) {
				return 0
			}
			return 1
		}
		if de.add != nil && !de.add.IsEmpty() {
			if de.del != nil && !de.del.IsEmpty() && de.add.Intersects(de.del) {
				bm, owned := composePostingOwned(base, de, nil)
				var out uint64
				if bm != nil {
					out = bm.GetCardinality()
				}
				if owned && bm != nil {
					releaseRoaringBuf(bm)
				}
				return out
			}
			if de.delSingleSet && de.add.Contains(de.delSingle) {
				addCard := de.add.GetCardinality()
				if addCard == 0 {
					return 0
				}
				return addCard - 1
			}
			return de.add.GetCardinality()
		}
		return 0
	}

	if de.addSingleSet {
		if de.delSingleSet && de.delSingle == de.addSingle {
			bm, owned := composePostingOwned(base, de, nil)
			var out uint64
			if bm != nil {
				out = bm.GetCardinality()
			}
			if owned && bm != nil {
				releaseRoaringBuf(bm)
			}
			return out
		}
		if de.del != nil && !de.del.IsEmpty() && de.del.Contains(de.addSingle) {
			bm, owned := composePostingOwned(base, de, nil)
			var out uint64
			if bm != nil {
				out = bm.GetCardinality()
			}
			if owned && bm != nil {
				releaseRoaringBuf(bm)
			}
			return out
		}
	}
	if de.delSingleSet && de.add != nil && !de.add.IsEmpty() && de.add.Contains(de.delSingle) {
		bm, owned := composePostingOwned(base, de, nil)
		var out uint64
		if bm != nil {
			out = bm.GetCardinality()
		}
		if owned && bm != nil {
			releaseRoaringBuf(bm)
		}
		return out
	}
	if de.add != nil && !de.add.IsEmpty() && de.del != nil && !de.del.IsEmpty() && de.add.Intersects(de.del) {
		bm, owned := composePostingOwned(base, de, nil)
		var out uint64
		if bm != nil {
			out = bm.GetCardinality()
		}
		if owned && bm != nil {
			releaseRoaringBuf(bm)
		}
		return out
	}

	var addNovel uint64
	if de.addSingleSet {
		if !base.Contains(de.addSingle) {
			addNovel = 1
		}
	} else if de.add != nil && !de.add.IsEmpty() {
		addCard := de.add.GetCardinality()
		if addCard > 0 {
			if base.isSingleton() {
				if de.add.Contains(base.single) {
					addNovel = addCard - 1
				} else {
					addNovel = addCard
				}
			} else {
				inter := base.bitmap().AndCardinality(de.add)
				if inter < addCard {
					addNovel = addCard - inter
				}
			}
		}
	}

	var delFromBase uint64
	if de.delSingleSet {
		if base.Contains(de.delSingle) {
			delFromBase = 1
		}
	} else if de.del != nil && !de.del.IsEmpty() {
		if base.isSingleton() {
			if de.del.Contains(base.single) {
				delFromBase = 1
			}
		} else {
			delFromBase = base.bitmap().AndCardinality(de.del)
		}
	}

	out := baseCard + addNovel
	if delFromBase >= out {
		return 0
	}
	return out - delFromBase
}

func resetOrNewBitmap(s *roaring64.Bitmap) *roaring64.Bitmap {
	if s == nil {
		return roaring64.NewBitmap()
	}
	s.Clear()
	return s
}

func (o fieldOverlay) rangeForBounds(b rangeBounds) overlayRange {
	br := overlayRange{
		baseStart:  0,
		baseEnd:    len(o.base),
		deltaStart: 0,
		deltaEnd:   0,
	}
	var dkeys []string
	if o.delta != nil {
		dkeys = o.delta.sortedKeys()
		br.deltaEnd = len(dkeys)
	}

	if b.hasPrefix {
		ps := lowerBoundIndex(o.base, b.prefix)
		pe := prefixRangeEndIndex(o.base, b.prefix, ps)
		br.baseStart = max(br.baseStart, ps)
		br.baseEnd = min(br.baseEnd, pe)

		if o.delta != nil {
			ds := lowerBoundStrings(dkeys, b.prefix)
			de := prefixRangeEndStrings(dkeys, b.prefix, ds)
			br.deltaStart = max(br.deltaStart, ds)
			br.deltaEnd = min(br.deltaEnd, de)
		}
	}

	if b.hasLo {
		bl := lowerBoundIndex(o.base, b.loKey)
		if !b.loInc && bl < len(o.base) && indexKeyEqualsString(o.base[bl].Key, b.loKey) {
			bl++
		}
		br.baseStart = max(br.baseStart, bl)

		if o.delta != nil {
			dl := lowerBoundStrings(dkeys, b.loKey)
			if !b.loInc && dl < len(dkeys) && dkeys[dl] == b.loKey {
				dl++
			}
			br.deltaStart = max(br.deltaStart, dl)
		}
	}

	if b.hasHi {
		var bh int
		if b.hiInc {
			bh = upperBoundIndex(o.base, b.hiKey)
		} else {
			bh = lowerBoundIndex(o.base, b.hiKey)
		}
		br.baseEnd = min(br.baseEnd, bh)

		if o.delta != nil {
			var dh int
			if b.hiInc {
				dh = upperBoundStrings(dkeys, b.hiKey)
			} else {
				dh = lowerBoundStrings(dkeys, b.hiKey)
			}
			br.deltaEnd = min(br.deltaEnd, dh)
		}
	}

	if br.baseStart < 0 {
		br.baseStart = 0
	}
	if br.baseEnd > len(o.base) {
		br.baseEnd = len(o.base)
	}
	if br.baseStart > br.baseEnd {
		br.baseStart = br.baseEnd
	}

	if o.delta != nil {
		if br.deltaStart < 0 {
			br.deltaStart = 0
		}
		if br.deltaEnd > len(dkeys) {
			br.deltaEnd = len(dkeys)
		}
		if br.deltaStart > br.deltaEnd {
			br.deltaStart = br.deltaEnd
		}
	} else {
		br.deltaStart = 0
		br.deltaEnd = 0
	}

	return br
}

func (o fieldOverlay) newCursor(br overlayRange, desc bool) overlayKeyCursor {
	c := overlayKeyCursor{
		base:   o.base,
		delta:  o.delta,
		desc:   desc,
		fixed8: o.fixed8,
	}
	if o.delta != nil {
		c.keys = o.delta.sortedKeys()
	}

	if !desc {
		c.b = br.baseStart
		c.be = br.baseEnd
		c.d = br.deltaStart
		c.de = br.deltaEnd
		return c
	}

	c.b = br.baseEnd - 1
	c.be = br.baseStart - 1
	c.d = br.deltaEnd - 1
	c.de = br.deltaStart - 1
	return c
}

func (c *overlayKeyCursor) next() (indexKey, postingList, indexDeltaEntry, bool) {
	if c.desc {
		return c.nextDesc()
	}
	return c.nextAsc()
}

func (c *overlayKeyCursor) nextAsc() (indexKey, postingList, indexDeltaEntry, bool) {
	hasBase := c.b < c.be
	hasDelta := c.delta != nil && c.d < c.de

	if !hasBase && !hasDelta {
		return indexKey{}, postingList{}, indexDeltaEntry{}, false
	}

	if hasBase && hasDelta {
		bk := c.base[c.b].Key
		dk := c.keys[c.d]
		cmp := compareIndexKeyString(bk, dk)
		switch {
		case cmp < 0:
			ids := c.base[c.b].IDs
			c.b++
			return bk, ids, indexDeltaEntry{}, true
		case cmp > 0:
			de, _ := c.delta.get(dk)
			c.d++
			return indexKeyFromStoredString(dk, c.fixed8), postingList{}, de, true
		default:
			ids := c.base[c.b].IDs
			de, _ := c.delta.get(dk)
			c.b++
			c.d++
			return bk, ids, de, true
		}
	}

	if hasBase {
		bk := c.base[c.b].Key
		ids := c.base[c.b].IDs
		c.b++
		return bk, ids, indexDeltaEntry{}, true
	}

	dk := c.keys[c.d]
	de, _ := c.delta.get(dk)
	c.d++
	return indexKeyFromStoredString(dk, c.fixed8), postingList{}, de, true
}

func (c *overlayKeyCursor) nextDesc() (indexKey, postingList, indexDeltaEntry, bool) {
	hasBase := c.b > c.be
	hasDelta := c.delta != nil && c.d > c.de

	if !hasBase && !hasDelta {
		return indexKey{}, postingList{}, indexDeltaEntry{}, false
	}

	if hasBase && hasDelta {
		bk := c.base[c.b].Key
		dk := c.keys[c.d]
		cmp := compareIndexKeyString(bk, dk)
		switch {
		case cmp > 0:
			ids := c.base[c.b].IDs
			c.b--
			return bk, ids, indexDeltaEntry{}, true
		case cmp < 0:
			de, _ := c.delta.get(dk)
			c.d--
			return indexKeyFromStoredString(dk, c.fixed8), postingList{}, de, true
		default:
			ids := c.base[c.b].IDs
			de, _ := c.delta.get(dk)
			c.b--
			c.d--
			return bk, ids, de, true
		}
	}

	if hasBase {
		bk := c.base[c.b].Key
		ids := c.base[c.b].IDs
		c.b--
		return bk, ids, indexDeltaEntry{}, true
	}

	dk := c.keys[c.d]
	de, _ := c.delta.get(dk)
	c.d--
	return indexKeyFromStoredString(dk, c.fixed8), postingList{}, de, true
}

func (d *fieldIndexDelta) sortedKeys() []string {
	if d == nil || !d.hasEntries() {
		return nil
	}
	d.keysOnce.Do(func() {
		if d.byKey == nil {
			d.keys = []string{d.singleKey}
			return
		}
		keys := make([]string, 0, len(d.byKey))
		for k := range d.byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		d.keys = keys
	})
	return d.keys
}

func (d *fieldIndexDelta) invalidateSortedKeys() {
	if d == nil {
		return
	}
	d.keys = nil
	d.keysOnce = sync.Once{}
}

func (d *fieldIndexDelta) keyCount() int {
	if d == nil {
		return 0
	}
	if d.byKey != nil {
		return len(d.byKey)
	}
	if d.singleSet {
		return 1
	}
	return 0
}

func (d *fieldIndexDelta) hasEntries() bool {
	return d.keyCount() > 0
}

func (d *fieldIndexDelta) get(key string) (indexDeltaEntry, bool) {
	if d == nil {
		return indexDeltaEntry{}, false
	}
	if d.byKey != nil {
		v, ok := d.byKey[key]
		return v, ok
	}
	if d.singleSet && d.singleKey == key {
		return d.singleEntry, true
	}
	return indexDeltaEntry{}, false
}

func (d *fieldIndexDelta) set(key string, e indexDeltaEntry) {
	if d == nil {
		return
	}
	if d.byKey != nil {
		d.byKey[key] = e
		return
	}
	if !d.singleSet {
		d.singleKey = key
		d.singleEntry = e
		d.singleSet = true
		return
	}
	if d.singleKey == key {
		d.singleEntry = e
		return
	}
	d.byKey = make(map[string]indexDeltaEntry, 2)
	d.byKey[d.singleKey] = d.singleEntry
	d.byKey[key] = e
	d.singleKey = ""
	d.singleEntry = indexDeltaEntry{}
	d.singleSet = false
}

func (d *fieldIndexDelta) remove(key string) {
	if d == nil {
		return
	}
	if d.byKey != nil {
		delete(d.byKey, key)
		switch len(d.byKey) {
		case 0:
			d.byKey = nil
			d.singleKey = ""
			d.singleEntry = indexDeltaEntry{}
			d.singleSet = false
		case 1:
			for k, v := range d.byKey {
				d.singleKey = k
				d.singleEntry = v
				d.singleSet = true
				break
			}
			d.byKey = nil
		}
		return
	}
	if d.singleSet && d.singleKey == key {
		d.singleKey = ""
		d.singleEntry = indexDeltaEntry{}
		d.singleSet = false
	}
}

func (d *fieldIndexDelta) forEach(fn func(string, indexDeltaEntry)) {
	if d == nil || !d.hasEntries() || fn == nil {
		return
	}
	if d.byKey != nil {
		for k, v := range d.byKey {
			fn(k, v)
		}
		return
	}
	fn(d.singleKey, d.singleEntry)
}

func lowerBoundStrings(s []string, key string) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi) >> 1
		if s[mid] < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func upperBoundStrings(s []string, key string) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi) >> 1
		if s[mid] <= key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func prefixRangeEndStrings(s []string, prefix string, start int) int {
	if start < 0 || start >= len(s) {
		return start
	}
	if s[start] < prefix || len(s[start]) < len(prefix) || s[start][:len(prefix)] != prefix {
		return start
	}
	if upper, ok := nextPrefixUpperBound(prefix); ok {
		end := lowerBoundStrings(s, upper)
		if end < start {
			end = start
		}
		return end
	}

	end := start
	for end < len(s) {
		k := s[end]
		if len(k) < len(prefix) || k[:len(prefix)] != prefix {
			break
		}
		end++
	}
	return end
}

func deltaEntryHasAdd(e indexDeltaEntry) bool {
	return e.addSingleSet || e.add != nil
}

func deltaEntryHasDel(e indexDeltaEntry) bool {
	return e.delSingleSet || e.del != nil
}

func deltaEntryIsEmpty(e indexDeltaEntry) bool {
	return !deltaEntryHasAdd(e) && !deltaEntryHasDel(e)
}

func deltaEntryAddContains(e indexDeltaEntry, id uint64) bool {
	if e.addSingleSet {
		return e.addSingle == id
	}
	if e.add == nil {
		return false
	}
	return e.add.Contains(id)
}

func deltaEntryDelContains(e indexDeltaEntry, id uint64) bool {
	if e.delSingleSet {
		return e.delSingle == id
	}
	if e.del == nil {
		return false
	}
	return e.del.Contains(id)
}

func composePostingContains(base postingList, de indexDeltaEntry, id uint64) bool {
	if deltaEntryIsEmpty(de) {
		return base.Contains(id)
	}
	if deltaEntryDelContains(de, id) {
		return false
	}
	if base.Contains(id) {
		return true
	}
	return deltaEntryAddContains(de, id)
}

func deltaEntryNormalize(e indexDeltaEntry) indexDeltaEntry {
	if e.add != nil && e.add.IsEmpty() {
		e.add = nil
	}
	if e.del != nil && e.del.IsEmpty() {
		e.del = nil
	}
	if e.add != nil {
		e.addSingleSet = false
		e.addSingle = 0
	}
	if e.del != nil {
		e.delSingleSet = false
		e.delSingle = 0
	}
	return e
}

func deltaEntryApplyAddToBitmap(dst *roaring64.Bitmap, e indexDeltaEntry) {
	if dst == nil {
		return
	}
	if e.add != nil && !e.add.IsEmpty() {
		dst.Or(e.add)
	}
	if e.addSingleSet {
		dst.Add(e.addSingle)
	}
}

func deltaEntryApplyDelToBitmap(dst *roaring64.Bitmap, e indexDeltaEntry) {
	if dst == nil {
		return
	}
	if e.del != nil && !e.del.IsEmpty() {
		dst.AndNot(e.del)
	}
	if e.delSingleSet {
		dst.Remove(e.delSingle)
	}
}

// deltaEntryMerge merges newer delta entry onto older one and returns normalized effective entry.
func deltaEntryMerge(base indexDeltaEntry, newer indexDeltaEntry) indexDeltaEntry {
	add := newMutableDeltaSide(base.add, base.addSingle, base.addSingleSet)
	del := newMutableDeltaSide(base.del, base.delSingle, base.delSingleSet)

	if newer.addSingleSet {
		add.addID(newer.addSingle)
	}
	add.addBitmap(newer.add)

	if newer.delSingleSet {
		del.addID(newer.delSingle)
	}
	del.addBitmap(newer.del)

	neutralizeMutableDeltaSides(&add, &del)

	out := indexDeltaEntry{
		add:          add.bmOrNil(),
		del:          del.bmOrNil(),
		addSingle:    add.single,
		delSingle:    del.single,
		addSingleSet: add.hasSingle,
		delSingleSet: del.hasSingle,
	}
	return deltaEntryNormalize(out)
}

// deltaEntryMergeOwned merges newer delta onto base assuming base bitmaps
// are exclusively owned by caller and can be modified in place.
func deltaEntryMergeOwned(base indexDeltaEntry, newer indexDeltaEntry) indexDeltaEntry {
	add := newMutableDeltaSide(base.add, base.addSingle, base.addSingleSet)
	del := newMutableDeltaSide(base.del, base.delSingle, base.delSingleSet)
	if add.bm != nil {
		add.owned = true
	}
	if del.bm != nil {
		del.owned = true
	}

	if newer.addSingleSet {
		add.addID(newer.addSingle)
	}
	add.addBitmap(newer.add)

	if newer.delSingleSet {
		del.addID(newer.delSingle)
	}
	del.addBitmap(newer.del)

	neutralizeMutableDeltaSides(&add, &del)

	out := indexDeltaEntry{
		add:          add.bmOrNil(),
		del:          del.bmOrNil(),
		addSingle:    add.single,
		delSingle:    del.single,
		addSingleSet: add.hasSingle,
		delSingleSet: del.hasSingle,
	}
	return deltaEntryNormalize(out)
}

// applyFieldDelta returns a new immutable delta from previous snapshot delta and per-key changes.
// Previous delta is not mutated.
func applyFieldDelta(prev *fieldIndexDelta, changes map[string]indexDeltaEntry) *fieldIndexDelta {
	if len(changes) == 0 {
		return prev
	}

	next := cloneFieldIndexDeltaShallow(prev)
	applyFieldDeltaInPlace(next, changes)
	return next
}

func applyFieldDeltaInPlace(next *fieldIndexDelta, changes map[string]indexDeltaEntry) {
	if next == nil || len(changes) == 0 {
		return
	}
	for key, ch := range changes {
		applyFieldDeltaEntryInPlace(next, key, ch)
	}
	next.invalidateSortedKeys()
}

func applyFieldDeltaInPlaceFromDelta(next, src *fieldIndexDelta) {
	if next == nil || src == nil {
		return
	}
	if src.fixed8 {
		next.fixed8 = true
	}
	if !src.hasEntries() {
		return
	}
	src.forEach(func(key string, ch indexDeltaEntry) {
		applyFieldDeltaEntryInPlace(next, key, ch)
	})
	next.invalidateSortedKeys()
}

func applyFieldDeltaEntryInPlace(next *fieldIndexDelta, key string, ch indexDeltaEntry) {
	if next == nil {
		return
	}
	cur, ok := next.get(key)
	if !ok {
		cur = indexDeltaEntry{}
	}
	oldOps := deltaEntryOps(cur)

	add := newMutableDeltaSide(cur.add, cur.addSingle, cur.addSingleSet)
	del := newMutableDeltaSide(cur.del, cur.delSingle, cur.delSingleSet)

	if ch.addSingleSet {
		add.addID(ch.addSingle)
	}
	add.addBitmap(ch.add)

	if ch.delSingleSet {
		del.addID(ch.delSingle)
	}
	del.addBitmap(ch.del)

	neutralizeMutableDeltaSides(&add, &del)

	updated := indexDeltaEntry{
		add:          add.bmOrNil(),
		del:          del.bmOrNil(),
		addSingle:    add.single,
		delSingle:    del.single,
		addSingleSet: add.hasSingle,
		delSingleSet: del.hasSingle,
	}
	updated = deltaEntryNormalize(updated)

	if deltaEntryIsEmpty(updated) {
		next.remove(key)
		if next.ops >= oldOps {
			next.ops -= oldOps
		} else {
			next.ops = 0
		}
		return
	}

	next.set(key, updated)
	newOps := deltaEntryOps(updated)
	if next.ops >= oldOps {
		next.ops -= oldOps
	} else {
		next.ops = 0
	}
	next.ops += newOps
}

type mutableDeltaSide struct {
	bm        *roaring64.Bitmap
	single    uint64
	hasSingle bool
	owned     bool
}

func newMutableDeltaSide(bm *roaring64.Bitmap, single uint64, hasSingle bool) mutableDeltaSide {
	return mutableDeltaSide{
		bm:        nonEmptyOrNil(bm),
		single:    single,
		hasSingle: hasSingle,
	}
}

func (s *mutableDeltaSide) hasData() bool {
	return s.hasSingle || (s.bm != nil && !s.bm.IsEmpty())
}

func (s *mutableDeltaSide) contains(id uint64) bool {
	if s.hasSingle {
		return s.single == id
	}
	if s.bm == nil {
		return false
	}
	return s.bm.Contains(id)
}

func (s *mutableDeltaSide) ensureOwned() {
	if s.bm == nil || s.owned {
		return
	}
	s.bm = cloneBitmap(s.bm)
	s.owned = true
}

func (s *mutableDeltaSide) addID(id uint64) {
	if s.bm != nil {
		s.ensureOwned()
		s.bm.Add(id)
		return
	}
	if !s.hasSingle {
		s.single = id
		s.hasSingle = true
		return
	}
	if s.single == id {
		return
	}
	s.bm = getRoaringBuf()
	s.bm.Add(s.single)
	s.bm.Add(id)
	s.hasSingle = false
	s.single = 0
	s.owned = true
}

func (s *mutableDeltaSide) addBitmap(src *roaring64.Bitmap) {
	if src == nil || src.IsEmpty() {
		return
	}
	if s.bm != nil {
		s.ensureOwned()
		s.bm.Or(src)
		return
	}
	if s.hasSingle {
		s.bm = getRoaringBuf()
		s.bm.Add(s.single)
		s.bm.Or(src)
		s.hasSingle = false
		s.single = 0
		s.owned = true
		return
	}
	s.bm = cloneBitmap(src)
	s.owned = true
}

func (s *mutableDeltaSide) removeID(id uint64) {
	if s.hasSingle {
		if s.single == id {
			s.hasSingle = false
			s.single = 0
		}
		return
	}
	if s.bm == nil || !s.bm.Contains(id) {
		return
	}
	s.ensureOwned()
	s.bm.Remove(id)
	s.normalize()
}

func (s *mutableDeltaSide) normalize() {
	if s.bm == nil {
		return
	}
	if s.bm.IsEmpty() {
		if s.owned {
			releaseRoaringBuf(s.bm)
		}
		s.bm = nil
		s.owned = false
		return
	}
	if s.bm.GetCardinality() == 1 {
		s.single = s.bm.Minimum()
		s.hasSingle = true
		if s.owned {
			releaseRoaringBuf(s.bm)
		}
		s.bm = nil
		s.owned = false
	}
}

func (s *mutableDeltaSide) bmOrNil() *roaring64.Bitmap {
	if s.bm != nil && s.bm.IsEmpty() {
		return nil
	}
	return s.bm
}

func neutralizeMutableDeltaSides(add, del *mutableDeltaSide) {
	if add == nil || del == nil || !add.hasData() || !del.hasData() {
		return
	}

	if add.hasSingle && del.contains(add.single) {
		del.removeID(add.single)
		add.hasSingle = false
		add.single = 0
	}
	if del.hasSingle && add.contains(del.single) {
		add.removeID(del.single)
		del.hasSingle = false
		del.single = 0
	}

	if add.bm != nil && del.bm != nil && add.bm.Intersects(del.bm) {
		add.ensureOwned()
		del.ensureOwned()
		common := getRoaringBuf()
		common.Or(add.bm)
		common.And(del.bm)
		if common != nil && !common.IsEmpty() {
			add.bm.AndNot(common)
			del.bm.AndNot(common)
		}
		releaseRoaringBuf(common)
	}

	add.normalize()
	del.normalize()
}

func cloneFieldIndexDeltaShallow(src *fieldIndexDelta) *fieldIndexDelta {
	if src == nil {
		return &fieldIndexDelta{}
	}
	if src.byKey == nil {
		if !src.singleSet {
			return &fieldIndexDelta{fixed8: src.fixed8, ops: src.ops}
		}
		return &fieldIndexDelta{
			singleKey:   src.singleKey,
			singleEntry: src.singleEntry,
			singleSet:   true,
			fixed8:      src.fixed8,
			ops:         src.ops,
		}
	}
	byKey := make(map[string]indexDeltaEntry, len(src.byKey))
	for k, v := range src.byKey {
		byKey[k] = v
	}
	return &fieldIndexDelta{
		byKey:  byKey,
		fixed8: src.fixed8,
		ops:    src.ops,
	}
}

func deltaEntryOps(e indexDeltaEntry) uint64 {
	var out uint64
	if e.add != nil {
		out += e.add.GetCardinality()
	}
	if e.addSingleSet {
		out++
	}
	if e.del != nil {
		out += e.del.GetCardinality()
	}
	if e.delSingleSet {
		out++
	}
	return out
}

func nonEmptyOrNil(bm *roaring64.Bitmap) *roaring64.Bitmap {
	if bm == nil || bm.IsEmpty() {
		return nil
	}
	return bm
}
