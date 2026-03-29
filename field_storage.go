package rbi

import (
	"slices"
	"sort"

	"github.com/vapstack/rbi/internal/posting"
)

const (
	fieldIndexChunkTargetEntries = 192
	fieldIndexChunkMinEntries    = fieldIndexChunkTargetEntries / 2
	fieldIndexChunkThreshold     = fieldIndexChunkTargetEntries * 2
	fieldIndexDirPageTargetRefs  = fieldIndexChunkTargetEntries
)

type fieldIndexStringRef struct {
	off uint32
	len uint32
}

type fieldIndexChunk struct {
	posts      []posting.List
	numeric    []uint64
	stringRefs []fieldIndexStringRef
	stringData []byte
	rows       uint64
}

type fieldIndexChunkRef struct {
	last  indexKey
	chunk *fieldIndexChunk
}

type fieldIndexChunkDirPage struct {
	refs      []fieldIndexChunkRef
	prefix    []int
	rowPrefix []uint64
}

// fieldIndexChunkedRoot stores immutable directory pages over immutable chunks
// plus top-level chunk/entry prefixes for fast rank/span calculations.
type fieldIndexChunkedRoot struct {
	pages       []fieldIndexChunkDirPage
	chunkPrefix []int
	prefix      []int
	rowPrefix   []uint64
	keyCount    int
	chunkCount  int
}

type fieldIndexChunkPos struct {
	chunk int
	entry int
}

// fieldIndexStorage is a compact nil-based union for one field index backend.
type fieldIndexStorage struct {
	flat    *[]index
	chunked *fieldIndexChunkedRoot
}

type fieldIndexChunkBuilder struct {
	pages       []fieldIndexChunkDirPage
	pendingRefs []fieldIndexChunkRef
	total       int
	chunks      int
}

type fieldIndexChunkStreamBuilder struct {
	builder           *fieldIndexChunkBuilder
	numeric           bool
	posts             []posting.List
	rows              uint64
	numericKey        []uint64
	stringKeys        []indexKey
	pendingPosts      []posting.List
	pendingRows       uint64
	pendingNumericKey []uint64
	pendingStringKeys []indexKey
}

func postingRows(posts []posting.List) uint64 {
	var rows uint64
	for i := range posts {
		rows += posts[i].Cardinality()
	}
	return rows
}

func newFlatFieldIndexStorage(slice *[]index) fieldIndexStorage {
	if slice == nil {
		return fieldIndexStorage{}
	}
	return fieldIndexStorage{flat: slice}
}

func newChunkedFieldIndexStorage(root *fieldIndexChunkedRoot) fieldIndexStorage {
	if root == nil || root.keyCount == 0 {
		return fieldIndexStorage{}
	}
	return fieldIndexStorage{chunked: root}
}

func newRegularFieldIndexStorage(slice *[]index) fieldIndexStorage {
	if slice == nil || len(*slice) == 0 {
		return fieldIndexStorage{}
	}
	if !shouldUseChunkedFieldIndex(len(*slice)) {
		return newFlatFieldIndexStorage(slice)
	}
	return newChunkedFieldIndexStorage(buildChunkedFieldIndexRoot(*slice))
}

func releaseFieldIndexStorageOwned(s fieldIndexStorage) {
	if s.flat != nil {
		entries := *s.flat
		for i := range entries {
			entries[i].IDs.Release()
			entries[i].IDs = posting.List{}
		}
		*s.flat = nil
		return
	}
	if s.chunked == nil {
		return
	}
	for i := range s.chunked.pages {
		page := &s.chunked.pages[i]
		for j := range page.refs {
			chunk := page.refs[j].chunk
			if chunk == nil {
				continue
			}
			posting.ReleaseSliceOwned(chunk.posts)
			chunk.posts = nil
			chunk.numeric = nil
			chunk.stringRefs = nil
			chunk.stringData = nil
			chunk.rows = 0
			page.refs[j].chunk = nil
		}
		page.refs = nil
		page.prefix = nil
		page.rowPrefix = nil
	}
	s.chunked.pages = nil
	s.chunked.chunkPrefix = nil
	s.chunked.prefix = nil
	s.chunked.rowPrefix = nil
	s.chunked.keyCount = 0
	s.chunked.chunkCount = 0
}

func releaseFieldIndexStorageMapOwned(m map[string]fieldIndexStorage) {
	if m == nil {
		return
	}
	for key, storage := range m {
		releaseFieldIndexStorageOwned(storage)
		delete(m, key)
	}
}

func newFlatFieldIndexStorageFromPostingMapOwned(m map[string]posting.List, fixed8 bool) fieldIndexStorage {
	if len(m) == 0 {
		releasePostingMap(m)
		return fieldIndexStorage{}
	}
	keys := make([]string, 0, len(m))
	for key, ids := range m {
		ids.Optimize()
		if ids.IsEmpty() {
			continue
		}
		m[key] = ids
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		releasePostingMap(m)
		return fieldIndexStorage{}
	}
	sort.Strings(keys)
	entries := make([]index, 0, len(keys))
	for i := range keys {
		entries = append(entries, index{
			Key: indexKeyFromStoredString(keys[i], fixed8),
			IDs: m[keys[i]],
		})
	}
	releasePostingMap(m)
	return newFlatFieldIndexStorage(&entries)
}

func newRegularFieldIndexStorageFromPostingMapOwned(m map[string]posting.List, fixed8 bool) fieldIndexStorage {
	if len(m) == 0 {
		releasePostingMap(m)
		return fieldIndexStorage{}
	}
	keys := make([]string, 0, len(m))
	for key, ids := range m {
		ids.Optimize()
		if ids.IsEmpty() {
			continue
		}
		m[key] = ids
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		releasePostingMap(m)
		return fieldIndexStorage{}
	}
	sort.Strings(keys)
	if !shouldUseChunkedFieldIndex(len(keys)) {
		entries := make([]index, 0, len(keys))
		for i := range keys {
			entries = append(entries, index{
				Key: indexKeyFromStoredString(keys[i], fixed8),
				IDs: m[keys[i]],
			})
		}
		releasePostingMap(m)
		return newFlatFieldIndexStorage(&entries)
	}

	builder := newFieldIndexChunkBuilder(len(keys))
	for start := 0; start < len(keys); {
		size := balancedFieldIndexChunkSize(len(keys) - start)
		end := start + size
		if fixed8 {
			numeric := make([]uint64, end-start)
			posts := make([]posting.List, end-start)
			for i, key := range keys[start:end] {
				numeric[i] = fixed8StringToU64(key)
				posts[i] = m[key]
			}
			builder.appendChunk(&fieldIndexChunk{
				posts:   posts,
				numeric: numeric,
				rows:    postingRows(posts),
			})
		} else {
			totalBytes := 0
			for _, key := range keys[start:end] {
				totalBytes += len(key)
			}
			data := make([]byte, totalBytes)
			refs := make([]fieldIndexStringRef, end-start)
			posts := make([]posting.List, end-start)
			off := 0
			for i, key := range keys[start:end] {
				n := copy(data[off:], key)
				refs[i] = fieldIndexStringRef{off: uint32(off), len: uint32(n)}
				posts[i] = m[key]
				off += n
			}
			builder.appendChunk(&fieldIndexChunk{
				posts:      posts,
				stringRefs: refs,
				stringData: data,
				rows:       postingRows(posts),
			})
		}
		start = end
	}
	releasePostingMap(m)
	return newChunkedFieldIndexStorage(builder.root())
}

func newFlatFieldIndexStorageFromFixedPostingMapOwned(m map[uint64]posting.List) fieldIndexStorage {
	if len(m) == 0 {
		releaseFixedPostingMap(m)
		return fieldIndexStorage{}
	}
	keys := make([]uint64, 0, len(m))
	for key, ids := range m {
		ids.Optimize()
		if ids.IsEmpty() {
			continue
		}
		m[key] = ids
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		releaseFixedPostingMap(m)
		return fieldIndexStorage{}
	}
	slices.Sort(keys)
	entries := make([]index, 0, len(keys))
	for i := range keys {
		entries = append(entries, index{
			Key: indexKeyFromU64(keys[i]),
			IDs: m[keys[i]],
		})
	}
	releaseFixedPostingMap(m)
	return newFlatFieldIndexStorage(&entries)
}

func newRegularFieldIndexStorageFromFixedPostingMapOwned(m map[uint64]posting.List) fieldIndexStorage {
	if len(m) == 0 {
		releaseFixedPostingMap(m)
		return fieldIndexStorage{}
	}
	keys := make([]uint64, 0, len(m))
	for key, ids := range m {
		ids.Optimize()
		if ids.IsEmpty() {
			continue
		}
		m[key] = ids
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		releaseFixedPostingMap(m)
		return fieldIndexStorage{}
	}
	slices.Sort(keys)
	if !shouldUseChunkedFieldIndex(len(keys)) {
		entries := make([]index, 0, len(keys))
		for i := range keys {
			entries = append(entries, index{
				Key: indexKeyFromU64(keys[i]),
				IDs: m[keys[i]],
			})
		}
		releaseFixedPostingMap(m)
		return newFlatFieldIndexStorage(&entries)
	}

	builder := newFieldIndexChunkBuilder(len(keys))
	for start := 0; start < len(keys); {
		size := balancedFieldIndexChunkSize(len(keys) - start)
		end := start + size
		numeric := make([]uint64, end-start)
		posts := make([]posting.List, end-start)
		copy(numeric, keys[start:end])
		for i, key := range keys[start:end] {
			posts[i] = m[key]
		}
		builder.appendChunk(&fieldIndexChunk{
			posts:   posts,
			numeric: numeric,
			rows:    postingRows(posts),
		})
		start = end
	}
	releaseFixedPostingMap(m)
	return newChunkedFieldIndexStorage(builder.root())
}

func shouldUseChunkedFieldIndex(keyCount int) bool {
	return keyCount >= fieldIndexChunkThreshold
}

func (s fieldIndexStorage) isChunked() bool {
	return s.chunked != nil
}

func (s fieldIndexStorage) keyCount() int {
	if s.chunked != nil {
		return s.chunked.keyCount
	}
	if s.flat == nil {
		return 0
	}
	return len(*s.flat)
}

func (s fieldIndexStorage) flatSlice() *[]index {
	if s.chunked != nil {
		return nil
	}
	return s.flat
}

func (c *fieldIndexChunk) keyCount() int {
	if c == nil {
		return 0
	}
	if c.numeric != nil {
		return len(c.numeric)
	}
	return len(c.stringRefs)
}

func (c *fieldIndexChunk) keyAt(i int) indexKey {
	if c == nil || i < 0 || i >= c.keyCount() {
		return indexKey{}
	}
	if c.numeric != nil {
		return indexKeyFromU64(c.numeric[i])
	}
	ref := c.stringRefs[i]
	return indexKeyFromBytes(c.stringData[int(ref.off) : int(ref.off)+int(ref.len)])
}

func (c *fieldIndexChunk) postingAt(i int) posting.List {
	if c == nil || i < 0 || i >= len(c.posts) {
		return posting.List{}
	}
	return c.posts[i].Borrow()
}

func (c *fieldIndexChunk) rowCount() uint64 {
	if c == nil {
		return 0
	}
	return c.rows
}

func (c *fieldIndexChunk) rowsInRange(start, end int) uint64 {
	if c == nil {
		return 0
	}
	if start < 0 {
		start = 0
	}
	if end > len(c.posts) {
		end = len(c.posts)
	}
	if start >= end {
		return 0
	}
	var total uint64
	for i := start; i < end; i++ {
		total += c.posts[i].Cardinality()
	}
	return total
}

func newFieldIndexChunkFromEntries(entries []index) *fieldIndexChunk {
	if len(entries) == 0 {
		return nil
	}
	posts := make([]posting.List, len(entries))
	var rows uint64
	if entries[0].Key.isNumeric() {
		keys := make([]uint64, len(entries))
		for i := range entries {
			keys[i] = entries[i].Key.meta
			posts[i] = entries[i].IDs
			rows += entries[i].IDs.Cardinality()
		}
		return &fieldIndexChunk{
			posts:   posts,
			numeric: keys,
			rows:    rows,
		}
	}

	totalBytes := 0
	for i := range entries {
		totalBytes += entries[i].Key.byteLen()
	}
	refs := make([]fieldIndexStringRef, len(entries))
	data := make([]byte, totalBytes)
	off := 0
	for i := range entries {
		s := entries[i].Key.asUnsafeString()
		n := copy(data[off:], s)
		refs[i] = fieldIndexStringRef{off: uint32(off), len: uint32(n)}
		posts[i] = entries[i].IDs
		rows += entries[i].IDs.Cardinality()
		off += n
	}
	return &fieldIndexChunk{
		posts:      posts,
		stringRefs: refs,
		stringData: data,
		rows:       rows,
	}
}

func lowerBoundFieldIndexChunk(chunk *fieldIndexChunk, key string) int {
	lo, hi := 0, chunk.keyCount()
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeyString(chunk.keyAt(mid), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func upperBoundFieldIndexChunk(chunk *fieldIndexChunk, key string) int {
	lo, hi := 0, chunk.keyCount()
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeyString(chunk.keyAt(mid), key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func balancedFieldIndexChunkSize(remaining int) int {
	if remaining <= fieldIndexChunkTargetEntries {
		return remaining
	}
	size := fieldIndexChunkTargetEntries
	tail := remaining - size
	if tail > 0 && tail < fieldIndexChunkMinEntries {
		size = remaining / 2
		if size < fieldIndexChunkMinEntries {
			size = fieldIndexChunkMinEntries
		}
		if size > fieldIndexChunkTargetEntries {
			size = fieldIndexChunkTargetEntries
		}
	}
	return size
}

func newFieldIndexChunkDirPage(refs []fieldIndexChunkRef) fieldIndexChunkDirPage {
	prefix := make([]int, len(refs)+1)
	rowPrefix := make([]uint64, len(refs)+1)
	total := 0
	rows := uint64(0)
	for i := range refs {
		total += refs[i].chunk.keyCount()
		rows += refs[i].chunk.rowCount()
		prefix[i+1] = total
		rowPrefix[i+1] = rows
	}
	return fieldIndexChunkDirPage{
		refs:      refs,
		prefix:    prefix,
		rowPrefix: rowPrefix,
	}
}

func (p fieldIndexChunkDirPage) keyCount() int {
	if len(p.prefix) == 0 {
		return 0
	}
	return p.prefix[len(p.prefix)-1]
}

func (p fieldIndexChunkDirPage) lastKey() indexKey {
	if len(p.refs) == 0 {
		return indexKey{}
	}
	return p.refs[len(p.refs)-1].last
}

func newFieldIndexChunkStreamBuilder(builder *fieldIndexChunkBuilder, numeric bool) fieldIndexChunkStreamBuilder {
	out := fieldIndexChunkStreamBuilder{
		builder: builder,
		numeric: numeric,
		posts:   make([]posting.List, 0, fieldIndexChunkTargetEntries),
	}
	if numeric {
		out.numericKey = make([]uint64, 0, fieldIndexChunkTargetEntries)
	} else {
		out.stringKeys = make([]indexKey, 0, fieldIndexChunkTargetEntries)
	}
	return out
}

func (b *fieldIndexChunkStreamBuilder) append(key indexKey, ids posting.List) {
	if b == nil || b.builder == nil || ids.IsEmpty() {
		return
	}
	b.posts = append(b.posts, ids)
	b.rows += ids.Cardinality()
	if b.numeric {
		b.numericKey = append(b.numericKey, key.meta)
	} else {
		b.stringKeys = append(b.stringKeys, key)
	}
	if len(b.posts) == fieldIndexChunkTargetEntries {
		b.sealFullChunk()
	}
}

func (b *fieldIndexChunkStreamBuilder) publishPendingChunk() {
	if b == nil || b.builder == nil || len(b.pendingPosts) == 0 {
		return
	}
	if b.numeric {
		b.builder.appendChunk(&fieldIndexChunk{
			posts:   b.pendingPosts,
			numeric: b.pendingNumericKey,
			rows:    b.pendingRows,
		})
	} else {
		b.builder.appendChunk(newFieldIndexChunkFromKeys(b.pendingPosts, b.pendingStringKeys, b.pendingRows))
	}
	b.pendingPosts = nil
	b.pendingRows = 0
	b.pendingNumericKey = nil
	b.pendingStringKeys = nil
}

func (b *fieldIndexChunkStreamBuilder) sealFullChunk() {
	if b == nil || b.builder == nil || len(b.posts) != fieldIndexChunkTargetEntries {
		return
	}
	b.publishPendingChunk()
	b.pendingPosts = b.posts
	b.pendingRows = b.rows
	if b.numeric {
		b.pendingNumericKey = b.numericKey
		b.numericKey = make([]uint64, 0, fieldIndexChunkTargetEntries)
	} else {
		b.pendingStringKeys = b.stringKeys
		b.stringKeys = make([]indexKey, 0, fieldIndexChunkTargetEntries)
	}
	b.posts = make([]posting.List, 0, fieldIndexChunkTargetEntries)
	b.rows = 0
}

func (b *fieldIndexChunkStreamBuilder) finish() {
	if b == nil {
		return
	}
	if len(b.pendingPosts) == 0 {
		if len(b.posts) == 0 {
			return
		}
		b.publishActiveChunk()
		return
	}
	if len(b.posts) == 0 {
		b.publishPendingChunk()
		return
	}
	if len(b.posts) >= fieldIndexChunkMinEntries {
		b.publishPendingChunk()
		b.publishActiveChunk()
		return
	}
	b.rebalancePendingWithTail()
}

func newFieldIndexChunkFromKeys(posts []posting.List, keys []indexKey, rows uint64) *fieldIndexChunk {
	if len(posts) == 0 {
		return nil
	}
	totalBytes := 0
	for i := range keys {
		totalBytes += keys[i].byteLen()
	}
	refs := make([]fieldIndexStringRef, len(keys))
	data := make([]byte, totalBytes)
	off := 0
	for i := range keys {
		n := copy(data[off:], keys[i].asUnsafeString())
		refs[i] = fieldIndexStringRef{off: uint32(off), len: uint32(n)}
		off += n
	}
	return &fieldIndexChunk{
		posts:      posts,
		stringRefs: refs,
		stringData: data,
		rows:       rows,
	}
}

func (b *fieldIndexChunkStreamBuilder) publishActiveChunk() {
	if b == nil || b.builder == nil || len(b.posts) == 0 {
		return
	}
	posts := slices.Clone(b.posts)
	if b.numeric {
		b.builder.appendChunk(&fieldIndexChunk{
			posts:   posts,
			numeric: slices.Clone(b.numericKey),
			rows:    b.rows,
		})
		b.numericKey = b.numericKey[:0]
	} else {
		b.builder.appendChunk(newFieldIndexChunkFromKeys(posts, b.stringKeys, b.rows))
		b.stringKeys = b.stringKeys[:0]
	}
	b.posts = b.posts[:0]
	b.rows = 0
}

func (b *fieldIndexChunkStreamBuilder) rebalancePendingWithTail() {
	if b == nil || b.builder == nil || len(b.pendingPosts) == 0 || len(b.posts) == 0 {
		return
	}
	total := len(b.pendingPosts) + len(b.posts)
	firstSize := balancedFieldIndexChunkSize(total)
	secondSize := total - firstSize
	combinedPosts := make([]posting.List, 0, total)
	combinedPosts = append(combinedPosts, b.pendingPosts...)
	combinedPosts = append(combinedPosts, b.posts...)
	firstPosts := slices.Clone(combinedPosts[:firstSize])
	secondPosts := slices.Clone(combinedPosts[firstSize:])
	if b.numeric {
		combinedKeys := make([]uint64, 0, total)
		combinedKeys = append(combinedKeys, b.pendingNumericKey...)
		combinedKeys = append(combinedKeys, b.numericKey...)
		b.builder.appendChunk(&fieldIndexChunk{
			posts:   firstPosts,
			numeric: slices.Clone(combinedKeys[:firstSize]),
			rows:    postingRows(firstPosts),
		})
		b.builder.appendChunk(&fieldIndexChunk{
			posts:   secondPosts,
			numeric: slices.Clone(combinedKeys[firstSize : firstSize+secondSize]),
			rows:    postingRows(secondPosts),
		})
		b.pendingNumericKey = nil
		b.numericKey = b.numericKey[:0]
	} else {
		combinedKeys := make([]indexKey, 0, total)
		combinedKeys = append(combinedKeys, b.pendingStringKeys...)
		combinedKeys = append(combinedKeys, b.stringKeys...)
		b.builder.appendChunk(newFieldIndexChunkFromKeys(firstPosts, combinedKeys[:firstSize], postingRows(firstPosts)))
		b.builder.appendChunk(newFieldIndexChunkFromKeys(secondPosts, combinedKeys[firstSize:firstSize+secondSize], postingRows(secondPosts)))
		b.pendingStringKeys = nil
		b.stringKeys = b.stringKeys[:0]
	}
	b.pendingPosts = nil
	b.pendingRows = 0
	b.posts = b.posts[:0]
	b.rows = 0
}

func newFieldIndexChunkBuilder(capEntries int) fieldIndexChunkBuilder {
	chunkCap := max(1, capEntries/fieldIndexChunkTargetEntries+1)
	pageCap := max(1, chunkCap/fieldIndexDirPageTargetRefs+2)
	return fieldIndexChunkBuilder{
		pages:       make([]fieldIndexChunkDirPage, 0, pageCap),
		pendingRefs: make([]fieldIndexChunkRef, 0, min(chunkCap, fieldIndexDirPageTargetRefs)),
	}
}

func (b *fieldIndexChunkBuilder) appendChunk(chunk *fieldIndexChunk) {
	if b == nil || chunk == nil || chunk.keyCount() == 0 {
		return
	}
	last := chunk.keyCount() - 1
	ref := fieldIndexChunkRef{
		last:  chunk.keyAt(last),
		chunk: chunk,
	}
	b.appendRef(ref)
}

func (b *fieldIndexChunkBuilder) appendRef(ref fieldIndexChunkRef) {
	if b == nil || ref.chunk == nil || ref.chunk.keyCount() == 0 {
		return
	}
	b.pendingRefs = append(b.pendingRefs, ref)
	size := ref.chunk.keyCount()
	b.total += size
	b.chunks++
	if len(b.pendingRefs) >= fieldIndexDirPageTargetRefs {
		b.flushPendingPage()
	}
}

func (b *fieldIndexChunkBuilder) appendRefsRange(root *fieldIndexChunkedRoot, start, end int) {
	if b == nil || root == nil || start < 0 || end < start || end > root.chunkCount || start == end {
		return
	}
	startPage, startOff := root.pagePosForChunk(start)
	endPage, endOff := root.pagePosForChunk(end)
	if startPage == len(root.pages) {
		return
	}
	if startPage == endPage {
		b.appendRefSlice(root.pages[startPage].refs[startOff:endOff])
		return
	}
	if startOff > 0 {
		b.appendRefSlice(root.pages[startPage].refs[startOff:])
		startPage++
	}
	for page := startPage; page < endPage; page++ {
		b.appendPage(root.pages[page])
	}
	if endOff > 0 {
		b.appendRefSlice(root.pages[endPage].refs[:endOff])
	}
}

func (b *fieldIndexChunkBuilder) appendEntries(entries []index) {
	if b == nil || len(entries) == 0 {
		return
	}
	for start := 0; start < len(entries); {
		size := balancedFieldIndexChunkSize(len(entries) - start)
		end := start + size
		b.appendChunk(newFieldIndexChunkFromEntries(entries[start:end]))
		start = end
	}
}

func (b *fieldIndexChunkBuilder) appendRefSlice(refs []fieldIndexChunkRef) {
	for i := range refs {
		b.appendRef(refs[i])
	}
}

func (b *fieldIndexChunkBuilder) flushPendingPage() {
	if b == nil || len(b.pendingRefs) == 0 {
		return
	}
	refs := append([]fieldIndexChunkRef(nil), b.pendingRefs...)
	b.pages = append(b.pages, newFieldIndexChunkDirPage(refs))
	b.pendingRefs = b.pendingRefs[:0]
}

func (b *fieldIndexChunkBuilder) appendPage(page fieldIndexChunkDirPage) {
	if b == nil || len(page.refs) == 0 {
		return
	}
	b.flushPendingPage()
	b.pages = append(b.pages, page)
	b.total += page.keyCount()
	b.chunks += len(page.refs)
}

func (b *fieldIndexChunkBuilder) root() *fieldIndexChunkedRoot {
	if b == nil || b.total == 0 {
		return nil
	}
	b.flushPendingPage()
	chunkPrefix := make([]int, len(b.pages)+1)
	prefix := make([]int, len(b.pages)+1)
	rowPrefix := make([]uint64, len(b.pages)+1)
	chunks := 0
	total := 0
	rows := uint64(0)
	for i := range b.pages {
		chunks += len(b.pages[i].refs)
		total += b.pages[i].keyCount()
		if len(b.pages[i].rowPrefix) > 0 {
			rows += b.pages[i].rowPrefix[len(b.pages[i].rowPrefix)-1]
		}
		chunkPrefix[i+1] = chunks
		prefix[i+1] = total
		rowPrefix[i+1] = rows
	}
	return &fieldIndexChunkedRoot{
		pages:       b.pages,
		chunkPrefix: chunkPrefix,
		prefix:      prefix,
		rowPrefix:   rowPrefix,
		keyCount:    total,
		chunkCount:  chunks,
	}
}

func buildChunkedFieldIndexRoot(entries []index) *fieldIndexChunkedRoot {
	if len(entries) == 0 {
		return nil
	}
	builder := newFieldIndexChunkBuilder(len(entries))
	builder.appendEntries(entries)
	return builder.root()
}

func flattenChunkedFieldIndexRoot(r *fieldIndexChunkedRoot) *[]index {
	if r == nil || r.keyCount == 0 {
		return nil
	}
	out := make([]index, 0, r.keyCount)
	for i := range r.pages {
		for j := range r.pages[i].refs {
			chunk := r.pages[i].refs[j].chunk
			for k := 0; k < chunk.keyCount(); k++ {
				out = append(out, index{
					Key: chunk.keyAt(k),
					IDs: chunk.postingAt(k),
				})
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return &out
}

func (r *fieldIndexChunkedRoot) startPos() fieldIndexChunkPos {
	return fieldIndexChunkPos{}
}

func (r *fieldIndexChunkedRoot) endPos() fieldIndexChunkPos {
	if r == nil {
		return fieldIndexChunkPos{}
	}
	return fieldIndexChunkPos{chunk: r.chunkCount}
}

func (r *fieldIndexChunkedRoot) isEndPos(pos fieldIndexChunkPos) bool {
	return r == nil || pos.chunk >= r.chunkCount
}

func (r *fieldIndexChunkedRoot) posKey(pos fieldIndexChunkPos) (indexKey, bool) {
	ref, ok := r.refAtChunk(pos.chunk)
	if !ok {
		return indexKey{}, false
	}
	if pos.entry < 0 || pos.entry >= ref.chunk.keyCount() {
		return indexKey{}, false
	}
	return ref.chunk.keyAt(pos.entry), true
}

func (r *fieldIndexChunkedRoot) advancePos(pos fieldIndexChunkPos) fieldIndexChunkPos {
	ref, ok := r.refAtChunk(pos.chunk)
	if !ok {
		return r.endPos()
	}
	if pos.entry+1 < ref.chunk.keyCount() {
		return fieldIndexChunkPos{chunk: pos.chunk, entry: pos.entry + 1}
	}
	nextChunk := pos.chunk + 1
	if nextChunk >= r.chunkCount {
		return r.endPos()
	}
	return fieldIndexChunkPos{chunk: nextChunk}
}

func (r *fieldIndexChunkedRoot) prevPos(end fieldIndexChunkPos) (fieldIndexChunkPos, bool) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, false
	}
	if end.chunk > r.chunkCount {
		end = r.endPos()
	}
	if end.chunk == r.chunkCount {
		lastChunk := r.chunkCount - 1
		ref, _ := r.refAtChunk(lastChunk)
		size := ref.chunk.keyCount()
		if size == 0 {
			return fieldIndexChunkPos{}, false
		}
		return fieldIndexChunkPos{chunk: lastChunk, entry: size - 1}, true
	}
	if end.chunk < 0 {
		return fieldIndexChunkPos{}, false
	}
	if end.entry > 0 {
		return fieldIndexChunkPos{chunk: end.chunk, entry: end.entry - 1}, true
	}
	if end.chunk == 0 {
		return fieldIndexChunkPos{}, false
	}
	prevChunk := end.chunk - 1
	ref, _ := r.refAtChunk(prevChunk)
	size := ref.chunk.keyCount()
	if size == 0 {
		return fieldIndexChunkPos{}, false
	}
	return fieldIndexChunkPos{chunk: prevChunk, entry: size - 1}, true
}

func (r *fieldIndexChunkedRoot) pagePosForChunk(chunk int) (int, int) {
	if chunk < 0 || chunk >= r.chunkCount || len(r.pages) == 0 {
		return len(r.pages), 0
	}
	page := sort.Search(len(r.pages), func(i int) bool {
		return r.chunkPrefix[i+1] > chunk
	})
	if page >= len(r.pages) {
		return len(r.pages), 0
	}
	return page, chunk - r.chunkPrefix[page]
}

func (r *fieldIndexChunkedRoot) refAtChunk(chunk int) (fieldIndexChunkRef, bool) {
	page, off := r.pagePosForChunk(chunk)
	if page >= len(r.pages) {
		return fieldIndexChunkRef{}, false
	}
	return r.pages[page].refs[off], true
}

func (r *fieldIndexChunkedRoot) entryPrefixForChunk(limit int) int {
	if r == nil || limit <= 0 {
		return 0
	}
	if limit >= r.chunkCount {
		return r.keyCount
	}
	page := sort.Search(len(r.pages), func(i int) bool {
		return r.chunkPrefix[i+1] >= limit
	})
	if page >= len(r.pages) {
		return r.keyCount
	}
	off := limit - r.chunkPrefix[page]
	return r.prefix[page] + r.pages[page].prefix[off]
}

func (r *fieldIndexChunkedRoot) rowPrefixForChunk(limit int) uint64 {
	if r == nil || limit <= 0 {
		return 0
	}
	if limit >= r.chunkCount {
		if len(r.rowPrefix) == 0 {
			return 0
		}
		return r.rowPrefix[len(r.rowPrefix)-1]
	}
	page := sort.Search(len(r.pages), func(i int) bool {
		return r.chunkPrefix[i+1] >= limit
	})
	if page >= len(r.pages) {
		if len(r.rowPrefix) == 0 {
			return 0
		}
		return r.rowPrefix[len(r.rowPrefix)-1]
	}
	off := limit - r.chunkPrefix[page]
	return r.rowPrefix[page] + r.pages[page].rowPrefix[off]
}

func (r *fieldIndexChunkedRoot) chunkRowsRange(start, end int) uint64 {
	if r == nil || start >= end {
		return 0
	}
	if start < 0 {
		start = 0
	}
	if end > r.chunkCount {
		end = r.chunkCount
	}
	if start >= end {
		return 0
	}
	return r.rowPrefixForChunk(end) - r.rowPrefixForChunk(start)
}

func (r *fieldIndexChunkedRoot) rangeRows(start, end fieldIndexChunkPos) uint64 {
	if r == nil || start.chunk > end.chunk || (start.chunk == end.chunk && start.entry >= end.entry) {
		return 0
	}
	if r.isEndPos(start) || start.chunk >= r.chunkCount {
		return 0
	}
	if start.chunk == end.chunk {
		ref, ok := r.refAtChunk(start.chunk)
		if !ok {
			return 0
		}
		return ref.chunk.rowsInRange(start.entry, end.entry)
	}

	var rows uint64
	startRef, ok := r.refAtChunk(start.chunk)
	if !ok {
		return 0
	}
	rows += startRef.chunk.rowsInRange(start.entry, startRef.chunk.keyCount())
	if start.chunk+1 < end.chunk {
		rows += r.chunkRowsRange(start.chunk+1, end.chunk)
	}
	if end.chunk < r.chunkCount {
		endRef, ok := r.refAtChunk(end.chunk)
		if ok {
			rows += endRef.chunk.rowsInRange(0, end.entry)
		}
	}
	return rows
}

func (r *fieldIndexChunkedRoot) posForRank(rank int) fieldIndexChunkPos {
	if r == nil || rank <= 0 || r.chunkCount == 0 {
		return fieldIndexChunkPos{}
	}
	if rank >= r.keyCount {
		return r.endPos()
	}
	page := sort.Search(len(r.pages), func(i int) bool {
		return r.prefix[i+1] > rank
	})
	if page >= len(r.pages) {
		return r.endPos()
	}
	localRank := rank - r.prefix[page]
	refIdx := sort.Search(len(r.pages[page].refs), func(i int) bool {
		return r.pages[page].prefix[i+1] > localRank
	})
	if refIdx >= len(r.pages[page].refs) {
		return r.endPos()
	}
	return fieldIndexChunkPos{
		chunk: r.chunkPrefix[page] + refIdx,
		entry: localRank - r.pages[page].prefix[refIdx],
	}
}

func (r *fieldIndexChunkedRoot) lowerBoundPos(key string) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, 0
	}
	page := sort.Search(len(r.pages), func(i int) bool {
		return compareIndexKeyString(r.pages[i].lastKey(), key) >= 0
	})
	if page >= len(r.pages) {
		return r.endPos(), r.keyCount
	}
	refs := r.pages[page].refs
	refIdx := sort.Search(len(refs), func(i int) bool {
		return compareIndexKeyString(refs[i].last, key) >= 0
	})
	if refIdx >= len(refs) {
		return r.endPos(), r.keyCount
	}
	chunk := refs[refIdx].chunk
	entryIdx := lowerBoundFieldIndexChunk(chunk, key)
	rank := r.prefix[page] + r.pages[page].prefix[refIdx] + entryIdx
	if entryIdx >= chunk.keyCount() {
		nextChunk := r.chunkPrefix[page] + refIdx + 1
		if nextChunk >= r.chunkCount {
			return r.endPos(), r.keyCount
		}
		return fieldIndexChunkPos{chunk: nextChunk}, rank
	}
	return fieldIndexChunkPos{chunk: r.chunkPrefix[page] + refIdx, entry: entryIdx}, rank
}

func (r *fieldIndexChunkedRoot) upperBoundPos(key string) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, 0
	}
	page := sort.Search(len(r.pages), func(i int) bool {
		return compareIndexKeyString(r.pages[i].lastKey(), key) > 0
	})
	if page >= len(r.pages) {
		return r.endPos(), r.keyCount
	}
	refs := r.pages[page].refs
	refIdx := sort.Search(len(refs), func(i int) bool {
		return compareIndexKeyString(refs[i].last, key) > 0
	})
	if refIdx >= len(refs) {
		return r.endPos(), r.keyCount
	}
	chunk := refs[refIdx].chunk
	entryIdx := upperBoundFieldIndexChunk(chunk, key)
	rank := r.prefix[page] + r.pages[page].prefix[refIdx] + entryIdx
	if entryIdx >= chunk.keyCount() {
		nextChunk := r.chunkPrefix[page] + refIdx + 1
		if nextChunk >= r.chunkCount {
			return r.endPos(), r.keyCount
		}
		return fieldIndexChunkPos{chunk: nextChunk}, rank
	}
	return fieldIndexChunkPos{chunk: r.chunkPrefix[page] + refIdx, entry: entryIdx}, rank
}

// prefixRangeEndPos returns the first position after prefix when start/startRank
// already points inside the prefix range; otherwise it returns the input pair.
func (r *fieldIndexChunkedRoot) prefixRangeEndPos(prefix string, start fieldIndexChunkPos, startRank int) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 || startRank >= r.keyCount {
		return start, startRank
	}
	if key, ok := r.posKey(start); !ok || compareIndexKeyString(key, prefix) < 0 || !indexKeyHasPrefixString(key, prefix) {
		return start, startRank
	}
	if upper, ok := nextPrefixUpperBound(prefix); ok {
		return r.lowerBoundPos(upper)
	}
	pos := start
	rank := startRank
	for !r.isEndPos(pos) {
		key, ok := r.posKey(pos)
		if !ok || !indexKeyHasPrefixString(key, prefix) {
			return pos, rank
		}
		pos = r.advancePos(pos)
		rank++
	}
	return pos, rank
}

func (r *fieldIndexChunkedRoot) lookupPostingRetained(key string) posting.List {
	if r == nil || r.chunkCount == 0 {
		return posting.List{}
	}
	page := sort.Search(len(r.pages), func(i int) bool {
		return compareIndexKeyString(r.pages[i].lastKey(), key) >= 0
	})
	if page >= len(r.pages) {
		return posting.List{}
	}
	refs := r.pages[page].refs
	refIdx := sort.Search(len(refs), func(i int) bool {
		return compareIndexKeyString(refs[i].last, key) >= 0
	})
	if refIdx >= len(refs) {
		return posting.List{}
	}
	chunk := refs[refIdx].chunk
	entryIdx := lowerBoundFieldIndexChunk(chunk, key)
	if entryIdx >= chunk.keyCount() || !indexKeyEqualsString(chunk.keyAt(entryIdx), key) {
		return posting.List{}
	}
	return chunk.postingAt(entryIdx)
}

func (r *fieldIndexChunkedRoot) lowerBound(key string) int {
	_, rank := r.lowerBoundPos(key)
	return rank
}

func (r *fieldIndexChunkedRoot) upperBound(key string) int {
	_, rank := r.upperBoundPos(key)
	return rank
}

func (r *fieldIndexChunkedRoot) prefixRangeEnd(prefix string, start int) int {
	if r == nil || r.chunkCount == 0 || start < 0 || start >= r.keyCount {
		return start
	}
	startPos := r.posForRank(start)
	startRank := start
	_, endRank := r.prefixRangeEndPos(prefix, startPos, startRank)
	return endRank
}

func (r *fieldIndexChunkedRoot) chunkRangeLen(start, end int) int {
	if r == nil || start < 0 || end < start || end > r.chunkCount {
		return 0
	}
	return r.entryPrefixForChunk(end) - r.entryPrefixForChunk(start)
}

func (r *fieldIndexChunkedRoot) touchChunkIndexFrom(start int, key indexKey) int {
	if r == nil || r.chunkCount == 0 {
		return -1
	}
	if start < 0 {
		start = 0
	}
	if start >= r.chunkCount {
		start = r.chunkCount - 1
	}
	startPage, startOff := r.pagePosForChunk(start)
	page := startPage + sort.Search(len(r.pages)-startPage, func(off int) bool {
		return compareIndexKeys(r.pages[startPage+off].lastKey(), key) >= 0
	})
	if page >= len(r.pages) {
		return r.chunkCount - 1
	}
	refs := r.pages[page].refs
	refStart := 0
	if page == startPage {
		refStart = startOff
	}
	refIdx := refStart + sort.Search(len(refs)-refStart, func(off int) bool {
		return compareIndexKeys(refs[refStart+off].last, key) >= 0
	})
	if refIdx >= len(refs) {
		return r.chunkPrefix[page+1] - 1
	}
	return r.chunkPrefix[page] + refIdx
}
