package strmap

import "sync"

type Snapshot struct {
	next      uint64
	keys      map[string]uint64
	strs      map[uint64]string
	denseStrs []string
	denseUsed []bool
	base      *Snapshot
	anchor    *Snapshot
	depth     int
	readDirs  []*readDir
	keysOnce  sync.Once
}

type Lookup struct {
	snap    *Snapshot
	pageIdx int
	page    *readPage
}

const (
	readPageShift = 8
	readPageSize  = 1 << readPageShift
	readDirShift  = 8
	readDirSize   = 1 << readDirShift
	readDirMask   = readDirSize - 1
)

type readDir struct {
	pages [readDirSize]*readPage
}

type readPage struct {
	start     uint64
	next      uint64
	strs      map[uint64]string
	denseStrs []string
	denseUsed []bool
}

func (s *Snapshot) Next() uint64 {
	return s.next
}

func (s *Snapshot) Index(key string) (uint64, bool) {
	return s.getIdxNoLock(key)
}

func (s *Snapshot) String(idx uint64) (string, bool) {
	return s.getStringNoLock(idx)
}

func (s *Snapshot) Lookup() Lookup {
	return Lookup{snap: s, pageIdx: -1}
}

func (l *Lookup) String(idx uint64) (string, bool) {
	s := l.snap
	if len(s.readDirs) > 0 {
		curPage := readPageIndex(idx)
		if curPage != l.pageIdx {
			l.pageIdx = curPage
			l.page = s.readPageAtNoLock(curPage)
		}
		if l.page != nil {
			if value, ok := l.page.getStringNoLock(idx); ok {
				return value, true
			}
		}
	}
	return s.getStringNoLock(idx)
}

func (s *Snapshot) baseNextNoLock() uint64 {
	if s.base == nil {
		return 0
	}
	return s.base.next
}

func (s *Snapshot) getOwnStringNoLock(idx uint64) (string, bool) {
	if idx > s.next {
		return "", false
	}
	if len(s.denseStrs) > 0 || len(s.denseUsed) > 0 {
		if idx > maxIntUint64 {
			return "", false
		}
		i := int(idx)
		if i < len(s.denseStrs) && i < len(s.denseUsed) && s.denseUsed[i] {
			return s.denseStrs[i], true
		}
		return "", false
	}
	if s.strs == nil {
		return "", false
	}
	v, ok := s.strs[idx]
	return v, ok
}

func denseWindowNoLock(strs []string, used []bool, start, next uint64) ([]string, []bool) {
	if start > next || start > maxIntUint64 {
		return nil, nil
	}
	limit := min(len(strs), len(used))
	if limit == 0 {
		return nil, nil
	}
	pos := int(start)
	if pos < 0 || pos >= limit {
		return nil, nil
	}
	end := limit
	if next < maxIntUint64 {
		maxPos := int(next) + 1
		if maxPos < end {
			end = maxPos
		}
	}
	if pos >= end {
		return nil, nil
	}
	return strs[pos:end], used[pos:end]
}

func newReadPageNoLock(node *Snapshot, start, next uint64) *readPage {
	if start > next {
		return nil
	}
	if len(node.denseStrs) > 0 || len(node.denseUsed) > 0 {
		denseStrs, denseUsed := denseWindowNoLock(node.denseStrs, node.denseUsed, start, next)
		if len(denseStrs) == 0 && len(denseUsed) == 0 {
			return nil
		}
		return &readPage{
			start:     start,
			next:      next,
			denseStrs: denseStrs,
			denseUsed: denseUsed,
		}
	}
	if node.strs == nil {
		return nil
	}
	return &readPage{
		start: start,
		next:  next,
		strs:  node.strs,
	}
}

func materializeReadPageNoLock(prefix *readPage, delta *Snapshot, deltaStart, start, next uint64) *readPage {
	if start > next || start > maxIntUint64 {
		return nil
	}
	size := int(next-start) + 1

	denseStrs := make([]string, size)
	denseUsed := make([]bool, size)

	if prefix != nil {
		prefixNext := min(next, deltaStart-1)
		for idx := start; idx <= prefixNext; idx++ {
			value, ok := prefix.getStringNoLock(idx)
			if !ok {
				continue
			}
			pos := int(idx - start)
			denseStrs[pos] = value
			denseUsed[pos] = true
		}
	}

	if delta != nil {
		deltaPos := max(start, deltaStart)
		for idx := deltaPos; idx <= next; idx++ {
			value, ok := delta.getOwnStringNoLock(idx)
			if !ok {
				continue
			}
			pos := int(idx - start)
			denseStrs[pos] = value
			denseUsed[pos] = true
		}
	}

	return &readPage{
		start:     start,
		next:      next,
		denseStrs: denseStrs,
		denseUsed: denseUsed,
	}
}

func buildSparsePageMapsNoLock(strs map[uint64]string, start, next uint64) map[int]map[uint64]string {
	if len(strs) == 0 || start > next {
		return nil
	}
	pageMaps := make(map[int]map[uint64]string)
	for idx, value := range strs {
		if idx < start || idx > next {
			continue
		}
		page := readPageIndex(idx)
		pageStrs := pageMaps[page]
		if pageStrs == nil {
			pageStrs = make(map[uint64]string)
			pageMaps[page] = pageStrs
		}
		pageStrs[idx] = value
	}
	return pageMaps
}

func (page *readPage) getStringNoLock(idx uint64) (string, bool) {
	if idx < page.start || idx > page.next {
		return "", false
	}
	if len(page.denseStrs) > 0 || len(page.denseUsed) > 0 {
		if idx-page.start > maxIntUint64 {
			return "", false
		}
		i := int(idx - page.start)
		if i < len(page.denseStrs) && i < len(page.denseUsed) && page.denseUsed[i] {
			return page.denseStrs[i], true
		}
		return "", false
	}
	if page.strs == nil {
		return "", false
	}
	v, ok := page.strs[idx]
	return v, ok
}

func (page *readPage) appendKeysNoLock(dst map[string]uint64) {
	if page == nil {
		return
	}
	if page.strs != nil {
		for idx, value := range page.strs {
			if idx >= page.start && idx <= page.next {
				dst[value] = idx
			}
		}
		return
	}
	limit := min(len(page.denseStrs), len(page.denseUsed))
	for i := 0; i < limit; i++ {
		if !page.denseUsed[i] {
			continue
		}
		dst[page.denseStrs[i]] = page.start + uint64(i)
	}
}

func (page *readPage) usedCountNoLock() int {
	if page == nil {
		return 0
	}
	if page.strs != nil {
		count := 0
		for idx := range page.strs {
			if idx >= page.start && idx <= page.next {
				count++
			}
		}
		return count
	}
	limit := min(len(page.denseStrs), len(page.denseUsed))
	count := 0
	for i := 0; i < limit; i++ {
		if !page.denseUsed[i] {
			continue
		}
		count++
	}
	return count
}

func readPageCount(next uint64) int {
	if next == 0 {
		return 0
	}
	return int(((next - 1) >> readPageShift) + 1)
}

func readPageIndex(idx uint64) int {
	return int((idx - 1) >> readPageShift)
}

func readPageBounds(page int, next uint64) (uint64, uint64) {
	start := uint64(page)<<readPageShift + 1
	end := start + readPageSize - 1
	if end > next {
		end = next
	}
	return start, end
}

func (s *Snapshot) readPageAtNoLock(page int) *readPage {
	if page < 0 || len(s.readDirs) == 0 {
		return nil
	}
	dirIdx := page >> readDirShift
	if dirIdx >= len(s.readDirs) {
		return nil
	}
	dir := s.readDirs[dirIdx]
	if dir == nil {
		return nil
	}
	return dir.pages[page&readDirMask]
}

func (s *Snapshot) readPageNoLock(idx uint64) *readPage {
	if idx == 0 || idx > s.next {
		return nil
	}
	return s.readPageAtNoLock(readPageIndex(idx))
}

func (s *Snapshot) buildKeysNoLock() map[string]uint64 {
	if s.next == 0 {
		return nil
	}

	usedCount := snapshotUsedCountNoLock(s)
	if usedCount == 0 {
		return nil
	}

	keys := make(map[string]uint64, usedCount)
	if len(s.readDirs) > 0 {
		for _, dir := range s.readDirs {
			if dir == nil {
				continue
			}
			for i := range dir.pages {
				dir.pages[i].appendKeysNoLock(keys)
			}
		}
		return keys
	}

	if s.base == nil {
		newReadPageNoLock(s, 1, s.next).appendKeysNoLock(keys)
		return keys
	}

	for cur := s; cur != nil; cur = cur.base {
		start := cur.baseNextNoLock() + 1
		if cur.base == nil {
			start = 1
		}
		newReadPageNoLock(cur, start, cur.next).appendKeysNoLock(keys)
	}
	return keys
}

func (s *Snapshot) ensureKeysNoLock() map[string]uint64 {
	s.keysOnce.Do(func() {
		if s.keys == nil {
			s.keys = s.buildKeysNoLock()
		}
	})
	return s.keys
}

func (s *Snapshot) getIdxNoLock(key string) (uint64, bool) {
	if len(s.readDirs) > 0 {
		keys := s.ensureKeysNoLock()
		if keys == nil {
			return 0, false
		}
		v, ok := keys[key]
		return v, ok
	}

	if s.base == nil {
		keys := s.ensureKeysNoLock()
		if keys == nil {
			return 0, false
		}
		v, ok := keys[key]
		return v, ok
	}

	for cur := s; cur != nil; cur = cur.base {
		if cur.base == nil {
			keys := cur.ensureKeysNoLock()
			if keys == nil {
				return 0, false
			}
			v, ok := keys[key]
			return v, ok
		}
		if cur.keys == nil {
			continue
		}
		if v, ok := cur.keys[key]; ok {
			return v, true
		}
	}
	return 0, false
}

func (s *Snapshot) getStringNoLock(idx uint64) (string, bool) {
	if idx == 0 || idx > s.next {
		return "", false
	}

	if len(s.readDirs) > 0 {
		page := s.readPageNoLock(idx)
		if page == nil {
			return "", false
		}
		return page.getStringNoLock(idx)
	}

	if s.base == nil {
		return s.getOwnStringNoLock(idx)
	}

	for cur := s; cur != nil; {
		if len(cur.denseStrs) > 0 || len(cur.denseUsed) > 0 {
			if value, ok := cur.getOwnStringNoLock(idx); ok {
				return value, true
			}
			if cur.base == nil {
				return "", false
			}
			if idx <= cur.base.next {
				if cur.anchor != nil && cur.anchor != cur.base && idx <= cur.anchor.next {
					cur = cur.anchor
				} else {
					cur = cur.base
				}
				continue
			}
			return "", false
		}
		if cur.strs != nil {
			v, ok := cur.strs[idx]
			if ok {
				return v, true
			}
		}
		if cur.base == nil {
			return "", false
		}
		if idx <= cur.base.next {
			if cur.anchor != nil && cur.anchor != cur.base && idx <= cur.anchor.next {
				cur = cur.anchor
			} else {
				cur = cur.base
			}
			continue
		}
		return "", false
	}
	return "", false
}

type readBuilder struct {
	dirs   []*readDir
	shared []*readDir
}

func newReadBuilder(pageCount int, shared []*readDir) readBuilder {
	dirCount := (pageCount + readDirSize - 1) >> readDirShift
	dirs := make([]*readDir, dirCount)
	if len(shared) > 0 {
		copy(dirs, shared[:min(len(shared), len(dirs))])
	}
	return readBuilder{
		dirs:   dirs,
		shared: shared,
	}
}

func (b *readBuilder) pageAtNoLock(page int) *readPage {
	if page < 0 {
		return nil
	}
	dirIdx := page >> readDirShift
	if dirIdx >= len(b.dirs) {
		return nil
	}
	dir := b.dirs[dirIdx]
	if dir == nil {
		return nil
	}
	return dir.pages[page&readDirMask]
}

func (b *readBuilder) setPageNoLock(page int, readPage *readPage) {
	if page < 0 {
		return
	}
	dirIdx := page >> readDirShift
	if dirIdx >= len(b.dirs) {
		return
	}
	dir := b.dirs[dirIdx]
	if dir == nil {
		dir = &readDir{}
		b.dirs[dirIdx] = dir
	} else if dirIdx < len(b.shared) && dir == b.shared[dirIdx] {
		cloned := *dir
		dir = &cloned
		b.dirs[dirIdx] = dir
	}
	dir.pages[page&readDirMask] = readPage
}

func appendReadPagesNoLock(builder *readBuilder, node *Snapshot, start, next uint64) {
	if start > next || next == 0 {
		return
	}

	firstPage := readPageIndex(start)
	lastPage := readPageIndex(next)
	if node.strs == nil {
		for page := firstPage; page <= lastPage; page++ {
			pageStart, pageNext := readPageBounds(page, next)
			if pageStart < start {
				existing := builder.pageAtNoLock(page)
				if existing != nil {
					builder.setPageNoLock(page, materializeReadPageNoLock(existing, node, start, pageStart, pageNext))
				} else {
					builder.setPageNoLock(page, newReadPageNoLock(node, start, pageNext))
				}
				continue
			}
			builder.setPageNoLock(page, newReadPageNoLock(node, pageStart, pageNext))
		}
		return
	}

	if firstPage == lastPage {
		pageStart, pageNext := readPageBounds(firstPage, next)
		if pageStart < start {
			existing := builder.pageAtNoLock(firstPage)
			if existing != nil {
				builder.setPageNoLock(firstPage, materializeReadPageNoLock(existing, node, start, pageStart, pageNext))
				return
			}
			pageStart = start
		}
		builder.setPageNoLock(firstPage, newReadPageNoLock(node, pageStart, pageNext))
		return
	}

	pageMaps := buildSparsePageMapsNoLock(node.strs, start, next)
	for page := firstPage; page <= lastPage; page++ {
		pageStart, pageNext := readPageBounds(page, next)
		if pageStart < start {
			existing := builder.pageAtNoLock(page)
			if existing != nil {
				builder.setPageNoLock(page, materializeReadPageNoLock(existing, node, start, pageStart, pageNext))
				continue
			}
			pageStart = start
		}
		pageStrs := pageMaps[page]
		if len(pageStrs) == 0 {
			continue
		}
		builder.setPageNoLock(page, &readPage{
			start: pageStart,
			next:  pageNext,
			strs:  pageStrs,
		})
	}
}

func buildPublishedSnapshotFromChain(state *Snapshot) *Snapshot {
	if state.next == 0 {
		return &Snapshot{}
	}
	depth := 0
	for cur := state; cur != nil; cur = cur.base {
		depth++
	}

	chain := make([]*Snapshot, depth)
	i := depth
	for cur := state; cur != nil; cur = cur.base {
		i--
		chain[i] = cur
	}
	chain = chain[i:]

	builder := newReadBuilder(readPageCount(state.next), nil)
	for _, node := range chain {
		start := node.baseNextNoLock() + 1
		if node.base == nil {
			start = 1
		}
		if node.next == 0 || start > node.next {
			continue
		}
		appendReadPagesNoLock(&builder, node, start, node.next)
	}
	return &Snapshot{
		next:     state.next,
		readDirs: builder.dirs,
	}
}

func buildPublishedSnapshotFromDelta(state *Snapshot, prev *Snapshot) *Snapshot {
	if state.next == 0 {
		return &Snapshot{}
	}
	if prev == nil || state.base == nil {
		return buildPublishedSnapshotFromChain(state)
	}

	builder := newReadBuilder(readPageCount(state.next), prev.readDirs)
	start := state.baseNextNoLock() + 1
	appendReadPagesNoLock(&builder, state, start, state.next)
	return &Snapshot{
		next:     state.next,
		readDirs: builder.dirs,
	}
}

func buildPublishedSnapshot(state, prevSource, prevPublished *Snapshot) *Snapshot {
	if state.next == 0 {
		return &Snapshot{}
	}
	if state.base == nil {
		return &Snapshot{
			next:      state.next,
			strs:      state.strs,
			denseStrs: state.denseStrs,
			denseUsed: state.denseUsed,
		}
	}
	if prevPublished != nil && prevSource == state.base && len(prevPublished.readDirs) > 0 {
		return buildPublishedSnapshotFromDelta(state, prevPublished)
	}
	return buildPublishedSnapshotFromChain(state)
}

func snapshotUsedCountNoLock(s *Snapshot) int {
	if len(s.readDirs) > 0 {
		count := 0
		for _, dir := range s.readDirs {
			if dir == nil {
				continue
			}
			for i := range dir.pages {
				count += dir.pages[i].usedCountNoLock()
			}
		}
		return count
	}
	return snapshotOwnUsedCount(s)
}

func snapshotOwnUsedCount(s *Snapshot) int {
	if len(s.keys) > 0 {
		return len(s.keys)
	}
	if s.strs != nil {
		return len(s.strs)
	}
	limit := min(len(s.denseStrs), len(s.denseUsed))
	if limit <= 1 {
		return 0
	}
	count := 0
	for i := 1; i < limit; i++ {
		if s.denseUsed[i] {
			count++
		}
	}
	return count
}
