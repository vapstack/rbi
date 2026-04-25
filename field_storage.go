package rbi

import (
	"fmt"
	"slices"
	"sort"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const (
	fieldIndexChunkTargetEntries = 192
	fieldIndexChunkMinEntries    = fieldIndexChunkTargetEntries / 2
	fieldIndexChunkMaxEntries    = fieldIndexChunkTargetEntries + fieldIndexChunkMinEntries
	fieldIndexChunkThreshold     = fieldIndexChunkTargetEntries * 2
	fieldIndexDirPageTargetRefs  = fieldIndexChunkTargetEntries
	fieldIndexStringRefMax       = 1<<16 - 1
)

type fieldIndexStringRef struct {
	off uint16
	len uint16
}

func indexedStringKeyTooLongLen(n int) bool {
	return n > fieldIndexStringRefMax
}

func validateIndexedStringKeyLen(n int) error {
	if indexedStringKeyTooLongLen(n) {
		return fmt.Errorf("indexed string value len %d exceeds limit %d", n, fieldIndexStringRefMax)
	}
	return nil
}

func fieldIndexStringRefFits(off, n int) bool {
	if n == 0 {
		return true
	}
	return off <= fieldIndexStringRefMax && n <= fieldIndexStringRefMax
}

func newFieldIndexStringRef(off, n int) fieldIndexStringRef {
	if n == 0 {
		return fieldIndexStringRef{}
	}
	if off > fieldIndexStringRefMax {
		panic("field index string ref offset exceeds uint16")
	}
	if n > fieldIndexStringRefMax {
		panic("field index string ref len exceeds uint16")
	}
	return fieldIndexStringRef{
		off: uint16(off),
		len: uint16(n),
	}
}

func nextStringFieldIndexChunkSizeStrings(keys []string, start int) int {
	target := balancedFieldIndexChunkSize(len(keys) - start)
	size := 0
	bytes := 0
	limit := min(len(keys)-start, fieldIndexChunkTargetEntries)
	for i := start; size < limit; i++ {
		keyLen := len(keys[i])
		if indexedStringKeyTooLongLen(keyLen) {
			panic(fmt.Sprintf("indexed string value len %d exceeds limit %d", keyLen, fieldIndexStringRefMax))
		}
		if size > 0 && keyLen > 0 && bytes > fieldIndexStringRefMax {
			break
		}
		size++
		bytes += keyLen
		if size == target {
			return size
		}
	}
	return size
}

func nextStringFieldIndexChunkSizeIndexKeys(keys []indexKey, start int) int {
	target := balancedFieldIndexChunkSize(len(keys) - start)
	size := 0
	bytes := 0
	limit := min(len(keys)-start, fieldIndexChunkTargetEntries)
	for i := start; size < limit; i++ {
		keyLen := keys[i].byteLen()
		if indexedStringKeyTooLongLen(keyLen) {
			panic(fmt.Sprintf("indexed string value len %d exceeds limit %d", keyLen, fieldIndexStringRefMax))
		}
		if size > 0 && keyLen > 0 && bytes > fieldIndexStringRefMax {
			break
		}
		size++
		bytes += keyLen
		if size == target {
			return size
		}
	}
	return size
}

func nextStringFieldIndexChunkSizeEntries(entries []index, start int) int {
	target := balancedFieldIndexChunkSize(len(entries) - start)
	size := 0
	bytes := 0
	limit := min(len(entries)-start, fieldIndexChunkTargetEntries)
	for i := start; size < limit; i++ {
		keyLen := entries[i].Key.byteLen()
		if indexedStringKeyTooLongLen(keyLen) {
			panic(fmt.Sprintf("indexed string value len %d exceeds limit %d", keyLen, fieldIndexStringRefMax))
		}
		if size > 0 && keyLen > 0 && bytes > fieldIndexStringRefMax {
			break
		}
		size++
		bytes += keyLen
		if size == target {
			return size
		}
	}
	return size
}

func stringEntriesFitSingleChunk(entries []index) bool {
	bytes := 0
	for i := range entries {
		keyLen := entries[i].Key.byteLen()
		if indexedStringKeyTooLongLen(keyLen) {
			return false
		}
		if i > 0 && keyLen > 0 && bytes > fieldIndexStringRefMax {
			return false
		}
		bytes += keyLen
	}
	return true
}

func shouldSealStringFieldIndexChunk(keyCount, bytes int) bool {
	return keyCount >= fieldIndexChunkTargetEntries || bytes > fieldIndexStringRefMax
}

type fieldIndexFlatRoot struct {
	refs    atomic.Int32
	entries []index
}

type fieldIndexChunk struct {
	refs       atomic.Int32
	posts      []posting.List
	numeric    []uint64
	stringRefs []fieldIndexStringRef
	stringData []byte
	rows       uint64
}

func (c *fieldIndexChunk) hasStringKeys() bool {
	return c != nil && c.stringRefs != nil
}

func (c *fieldIndexChunk) hasNumericKeys() bool {
	return c != nil && c.stringRefs == nil && c.numeric != nil
}

func (c *fieldIndexChunk) hasUniqueStringOwners() bool {
	return c != nil && c.stringRefs != nil && c.numeric != nil && c.posts == nil
}

func (c *fieldIndexChunk) hasUniqueNumericOwners() bool {
	return c != nil && c.stringRefs == nil && c.numeric != nil && c.posts == nil
}

type fieldIndexChunkRef struct {
	last  indexKey
	chunk *fieldIndexChunk
}

type fieldIndexChunkDirPage struct {
	refsCount atomic.Int32
	refs      *pooled.SliceBuf[fieldIndexChunkRef]
	prefix    *pooled.SliceBuf[int]
	rowPrefix *pooled.SliceBuf[uint64]
}

// fieldIndexChunkedRoot stores immutable directory pages over immutable chunks
// plus top-level chunk/entry prefixes for fast rank/span calculations.
type fieldIndexChunkedRoot struct {
	refs        atomic.Int32
	pages       *pooled.SliceBuf[*fieldIndexChunkDirPage]
	chunkPrefix *pooled.SliceBuf[int]
	prefix      *pooled.SliceBuf[int]
	rowPrefix   *pooled.SliceBuf[uint64]
	keyCount    int
	chunkCount  int
}

type fieldIndexChunkPos struct {
	chunk int
	entry int
}

// fieldIndexStorage is a compact nil-based union for one field index backend.
type fieldIndexStorage struct {
	flat    *fieldIndexFlatRoot
	chunked *fieldIndexChunkedRoot
}

type fieldIndexChunkBuilder struct {
	pages       *pooled.SliceBuf[*fieldIndexChunkDirPage]
	pendingRefs *pooled.SliceBuf[fieldIndexChunkRef]
	total       int
	chunks      int
}

type fieldIndexChunkStreamBuilder struct {
	builder           *fieldIndexChunkBuilder
	numeric           bool
	posts             []posting.List
	rows              uint64
	stringBytes       int
	numericKey        []uint64
	stringKeys        []indexKey
	freeStringKeys    []indexKey
	pendingPosts      []posting.List
	pendingRows       uint64
	pendingNumericKey []uint64
	pendingStringKeys []indexKey
}

var fieldIndexChunkRefSlicePool = pooled.Slices[fieldIndexChunkRef]{
	MinCap: fieldIndexDirPageTargetRefs,
	Clear:  true,
}

var fieldIndexChunkDirPagePtrSlicePool = pooled.Slices[*fieldIndexChunkDirPage]{
	MinCap: 8,
	Clear:  true,
}

var fieldIndexStorageSlicePool = pooled.Slices[fieldIndexStorage]{
	Clear: true,
}

var fieldIndexBoolSlicePool = pooled.Slices[bool]{
	Clear: true,
}

var fieldIndexOrdinalSlicePool = pooled.Slices[int]{}

var fieldIndexChunkDirPagePrefixPool = pooled.Slices[int]{
	MinCap: fieldIndexDirPageTargetRefs + 1,
}

var fieldIndexChunkDirPageRowPrefixPool = pooled.Slices[uint64]{
	MinCap: fieldIndexDirPageTargetRefs + 1,
}

var fieldIndexChunkRootChunkPrefixPool = pooled.Slices[int]{
	MinCap: 8,
}

var fieldIndexChunkRootPrefixPool = pooled.Slices[int]{
	MinCap: 8,
}

var fieldIndexChunkRootRowPrefixPool = pooled.Slices[uint64]{
	MinCap: 8,
}

var fieldIndexChunkDirPagePool = pooled.Pointers[fieldIndexChunkDirPage]{Clear: true}

func newFieldIndexChunkRefBuf(capHint int) *pooled.SliceBuf[fieldIndexChunkRef] {
	buf := fieldIndexChunkRefSlicePool.Get()
	if capHint > 0 {
		buf.Grow(capHint)
	}
	return buf
}

func newFieldIndexChunkDirPageSlice(capHint int) *pooled.SliceBuf[*fieldIndexChunkDirPage] {
	buf := fieldIndexChunkDirPagePtrSlicePool.Get()
	if capHint > 0 {
		buf.Grow(capHint)
	}
	return buf
}

func newFieldIndexChunkDirPageOwned(refs *pooled.SliceBuf[fieldIndexChunkRef]) *fieldIndexChunkDirPage {
	page := fieldIndexChunkDirPagePool.Get()
	page.refsCount.Store(1)
	page.refs = refs
	page.prefix = fieldIndexChunkDirPagePrefixPool.Get()
	page.prefix.SetLen(refs.Len() + 1)
	page.prefix.Set(0, 0)
	page.rowPrefix = fieldIndexChunkDirPageRowPrefixPool.Get()
	page.rowPrefix.SetLen(refs.Len() + 1)
	page.rowPrefix.Set(0, 0)

	total := 0
	rows := uint64(0)
	for i := 0; i < refs.Len(); i++ {
		ref := refs.Get(i)
		total += ref.chunk.keyCount()
		rows += ref.chunk.rowCount()
		page.prefix.Set(i+1, total)
		page.rowPrefix.Set(i+1, rows)
	}
	return page
}

func (p *fieldIndexChunkDirPage) retain() {
	p.refsCount.Add(1)
}

func newFieldIndexChunkDirPage(refs []fieldIndexChunkRef) *fieldIndexChunkDirPage {
	if len(refs) == 0 {
		return nil
	}
	buf := newFieldIndexChunkRefBuf(len(refs))
	buf.AppendAll(refs)
	return newFieldIndexChunkDirPageOwned(buf)
}

func (p *fieldIndexChunkDirPage) release() {
	if p == nil || p.refsCount.Add(-1) != 0 {
		return
	}
	if p.refs != nil {
		for i := 0; i < p.refs.Len(); i++ {
			ref := p.refs.Get(i)
			if ref.chunk != nil {
				ref.chunk.release()
			}
		}
		fieldIndexChunkRefSlicePool.Put(p.refs)
		p.refs = nil
	}
	if p.prefix != nil {
		fieldIndexChunkDirPagePrefixPool.Put(p.prefix)
		p.prefix = nil
	}
	if p.rowPrefix != nil {
		fieldIndexChunkDirPageRowPrefixPool.Put(p.rowPrefix)
		p.rowPrefix = nil
	}
	fieldIndexChunkDirPagePool.Put(p)
}

func (p *fieldIndexChunkDirPage) refsLen() int {
	if p == nil || p.refs == nil {
		return 0
	}
	return p.refs.Len()
}

func (p *fieldIndexChunkDirPage) refAt(i int) fieldIndexChunkRef {
	if p == nil || p.refs == nil {
		return fieldIndexChunkRef{}
	}
	return p.refs.Get(i)
}

func (p *fieldIndexChunkDirPage) keyCount() int {
	if p == nil || p.prefix == nil || p.prefix.Len() == 0 {
		return 0
	}
	return p.prefix.Get(p.prefix.Len() - 1)
}

func (p *fieldIndexChunkDirPage) prefixAt(i int) int {
	if p == nil || p.prefix == nil {
		return 0
	}
	return p.prefix.Get(i)
}

func (p *fieldIndexChunkDirPage) rowPrefixAt(i int) uint64 {
	if p == nil || p.rowPrefix == nil {
		return 0
	}
	return p.rowPrefix.Get(i)
}

func (p *fieldIndexChunkDirPage) lastKey() indexKey {
	if p == nil || p.refs == nil || p.refs.Len() == 0 {
		return indexKey{}
	}
	return p.refs.Get(p.refs.Len() - 1).last
}

func releaseFieldIndexChunkDirPages(pages *pooled.SliceBuf[*fieldIndexChunkDirPage]) {
	if pages == nil {
		return
	}
	for i := 0; i < pages.Len(); i++ {
		page := pages.Get(i)
		if page != nil {
			page.release()
		}
	}
	fieldIndexChunkDirPagePtrSlicePool.Put(pages)
}

func postingRows(posts []posting.List) uint64 {
	var rows uint64
	for i := range posts {
		rows += posts[i].Cardinality()
	}
	return rows
}

func storedFieldPosting(ids posting.List) posting.List {
	if ids.IsBorrowed() {
		return ids.Clone()
	}
	return ids
}

func borrowedFieldPosting(ids posting.List) posting.List {
	return ids.Borrow()
}

func borrowedFieldIndexEntry(ent index) index {
	ent.IDs = ent.IDs.Borrow()
	return ent
}

func copyBorrowedIndexEntries(dst, src []index) {
	for i := range src {
		dst[i] = borrowedFieldIndexEntry(src[i])
	}
}

func appendBorrowedIndexEntries(dst []index, src []index) []index {
	for i := range src {
		dst = append(dst, borrowedFieldIndexEntry(src[i]))
	}
	return dst
}

func copyBorrowedPostingSlice(dst, src []posting.List) {
	for i := range src {
		dst[i] = borrowedFieldPosting(src[i])
	}
}

func newNumericFieldIndexChunk(posts []posting.List, keys []uint64, rows uint64) *fieldIndexChunk {
	if chunkAllSingletonPosts(posts) {
		keyOwners := interleavedNumericChunkPairs(keys, posts)
		posting.ReleaseSliceOwned(posts)
		return newUniqueNumericFieldIndexChunk(keyOwners)
	}
	for i := range posts {
		posts[i] = storedFieldPosting(posts[i])
	}
	chunk := &fieldIndexChunk{
		posts:   posts,
		numeric: keys,
		rows:    rows,
	}
	chunk.refs.Store(1)
	return chunk
}

func chunkAllSingletonPosts(posts []posting.List) bool {
	for i := range posts {
		if _, ok := posts[i].TrySingle(); !ok {
			return false
		}
	}
	return true
}

func stringChunkOwners(posts []posting.List) []uint64 {
	owners := make([]uint64, len(posts))
	for i := range posts {
		owner, ok := posts[i].TrySingle()
		if !ok {
			panic("string owner chunk requires singleton postings")
		}
		owners[i] = owner
	}
	return owners
}

func interleavedNumericChunkPairs(keys []uint64, posts []posting.List) []uint64 {
	if len(keys) != len(posts) {
		panic("numeric owner chunk keys/posts len mismatch")
	}
	keyOwners := make([]uint64, len(keys)*2)
	for i := range keys {
		owner, ok := posts[i].TrySingle()
		if !ok {
			panic("numeric owner chunk requires singleton postings")
		}
		base := i << 1
		keyOwners[base] = keys[i]
		keyOwners[base+1] = owner
	}
	return keyOwners
}

func newUniqueNumericFieldIndexChunk(keyOwners []uint64) *fieldIndexChunk {
	if len(keyOwners)&1 != 0 {
		panic("numeric owner chunk key/owner data len mismatch")
	}
	chunk := &fieldIndexChunk{
		numeric: keyOwners,
		rows:    uint64(len(keyOwners) >> 1),
	}
	chunk.refs.Store(1)
	return chunk
}

func newUniqueStringFieldIndexChunk(owners []uint64, refs []fieldIndexStringRef, data []byte) *fieldIndexChunk {
	if len(owners) != len(refs) {
		panic("string owner chunk owners/refs len mismatch")
	}
	chunk := &fieldIndexChunk{
		numeric:    owners,
		stringRefs: refs,
		stringData: data,
		rows:       uint64(len(refs)),
	}
	chunk.refs.Store(1)
	return chunk
}

func newStringFieldIndexChunk(posts []posting.List, refs []fieldIndexStringRef, data []byte, rows uint64) *fieldIndexChunk {
	if chunkAllSingletonPosts(posts) {
		owners := stringChunkOwners(posts)
		posting.ReleaseSliceOwned(posts)
		return newUniqueStringFieldIndexChunk(owners, refs, data)
	}
	for i := range posts {
		posts[i] = storedFieldPosting(posts[i])
	}
	chunk := &fieldIndexChunk{
		posts:      posts,
		stringRefs: refs,
		stringData: data,
		rows:       rows,
	}
	chunk.refs.Store(1)
	return chunk
}

func newFlatFieldIndexStorage(slice *[]index) fieldIndexStorage {
	if slice == nil {
		return fieldIndexStorage{}
	}
	root := &fieldIndexFlatRoot{
		entries: *slice,
	}
	for i := range root.entries {
		root.entries[i].IDs = storedFieldPosting(root.entries[i].IDs)
	}
	root.refs.Store(1)
	return fieldIndexStorage{flat: root}
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

func (r *fieldIndexFlatRoot) retain() {
	r.refs.Add(1)
}

func (r *fieldIndexFlatRoot) release() {
	if r == nil || r.refs.Add(-1) != 0 {
		return
	}
	for i := range r.entries {
		r.entries[i].IDs.Release()
	}
}

func (c *fieldIndexChunk) retain() {
	c.refs.Add(1)
}

func (c *fieldIndexChunk) release() {
	if c == nil || c.refs.Add(-1) != 0 {
		return
	}
	posting.ReleaseSliceOwned(c.posts)
}

func (r *fieldIndexChunkedRoot) retain() {
	r.refs.Add(1)
}

func (r *fieldIndexChunkedRoot) release() {
	if r == nil || r.refs.Add(-1) != 0 {
		return
	}
	releaseFieldIndexChunkDirPages(r.pages)
	r.pages = nil
	if r.chunkPrefix != nil {
		fieldIndexChunkRootChunkPrefixPool.Put(r.chunkPrefix)
		r.chunkPrefix = nil
	}
	if r.prefix != nil {
		fieldIndexChunkRootPrefixPool.Put(r.prefix)
		r.prefix = nil
	}
	if r.rowPrefix != nil {
		fieldIndexChunkRootRowPrefixPool.Put(r.rowPrefix)
		r.rowPrefix = nil
	}
}

func (s fieldIndexStorage) retain() {
	if s.flat != nil {
		s.flat.retain()
		return
	}
	if s.chunked != nil {
		s.chunked.retain()
	}
}

func releaseFieldIndexStorageOwned(s fieldIndexStorage) {
	if s.flat != nil {
		s.flat.release()
		return
	}
	if s.chunked != nil {
		s.chunked.release()
	}
}

func releaseFieldIndexStorageMapOwned(m map[string]fieldIndexStorage) {
	if m == nil {
		return
	}
	for _, storage := range m {
		releaseFieldIndexStorageOwned(storage)
	}
}

func retainSharedFieldIndexStorageSlots(next, prev *pooled.SliceBuf[fieldIndexStorage]) {
	if next == nil || prev == nil {
		return
	}
	limit := min(next.Len(), prev.Len())
	for i := 0; i < limit; i++ {
		storage := next.Get(i)
		if storage == prev.Get(i) {
			storage.retain()
		}
	}
}

func releaseFieldIndexStorageSlotsOwned(slots *pooled.SliceBuf[fieldIndexStorage]) {
	if slots == nil {
		return
	}
	for i := 0; i < slots.Len(); i++ {
		releaseFieldIndexStorageOwned(slots.Get(i))
	}
	fieldIndexStorageSlicePool.Put(slots)
}

func newFlatFieldIndexStorageFromPostingMapOwned(m map[string]posting.List, fixed8 bool) fieldIndexStorage {
	if len(m) == 0 {
		postingMapPool.Put(m)
		return fieldIndexStorage{}
	}
	keys := make([]string, 0, len(m))
	for key, ids := range m {
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			continue
		}
		m[key] = ids
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		postingMapPool.Put(m)
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
	postingMapPool.Put(m)
	return newFlatFieldIndexStorage(&entries)
}

func newFlatFieldIndexStorageFromInsertPostingAccumsOwned(
	m map[string]uint32,
	arena *insertPostingAccumArena,
	fixed8 bool,
) fieldIndexStorage {
	if len(m) == 0 {
		insertPostingMapPool.Put(m)
		return fieldIndexStorage{}
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]index, 0, len(keys))
	for i := range keys {
		entries = append(entries, index{
			Key: indexKeyFromStoredString(keys[i], fixed8),
			IDs: arena.accum(m[keys[i]]).materializeOwned(),
		})
	}
	insertPostingMapPool.Put(m)
	return newFlatFieldIndexStorage(&entries)
}

func newRegularFieldIndexStorageFromPostingMapOwned(m map[string]posting.List, fixed8 bool) fieldIndexStorage {
	if len(m) == 0 {
		postingMapPool.Put(m)
		return fieldIndexStorage{}
	}
	keys := make([]string, 0, len(m))
	for key, ids := range m {
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			continue
		}
		m[key] = ids
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		postingMapPool.Put(m)
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
		postingMapPool.Put(m)
		return newFlatFieldIndexStorage(&entries)
	}

	builder := newFieldIndexChunkBuilder(len(keys))
	for start := 0; start < len(keys); {
		size := nextStringFieldIndexChunkSizeStrings(keys, start)
		end := start + size
		if fixed8 {
			numeric := make([]uint64, end-start)
			posts := make([]posting.List, end-start)
			for i, key := range keys[start:end] {
				numeric[i] = fixed8StringToU64(key)
				posts[i] = m[key]
			}
			builder.appendChunk(newNumericFieldIndexChunk(posts, numeric, postingRows(posts)))
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
				refs[i] = newFieldIndexStringRef(off, n)
				posts[i] = m[key]
				off += n
			}
			builder.appendChunk(newStringFieldIndexChunk(posts, refs, data, postingRows(posts)))
		}
		start = end
	}
	postingMapPool.Put(m)
	return newChunkedFieldIndexStorage(builder.root())
}

func newRegularFieldIndexStorageFromInsertPostingAccumsOwned(
	m map[string]uint32,
	arena *insertPostingAccumArena,
	fixed8 bool,
) fieldIndexStorage {
	if len(m) == 0 {
		insertPostingMapPool.Put(m)
		return fieldIndexStorage{}
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if !shouldUseChunkedFieldIndex(len(keys)) {
		entries := make([]index, 0, len(keys))
		for i := range keys {
			entries = append(entries, index{
				Key: indexKeyFromStoredString(keys[i], fixed8),
				IDs: arena.accum(m[keys[i]]).materializeOwned(),
			})
		}
		insertPostingMapPool.Put(m)
		return newFlatFieldIndexStorage(&entries)
	}

	builder := newFieldIndexChunkBuilder(len(keys))

	for start := 0; start < len(keys); {
		size := nextStringFieldIndexChunkSizeStrings(keys, start)
		end := start + size
		if fixed8 {
			numeric := make([]uint64, end-start)
			posts := make([]posting.List, end-start)
			for i, key := range keys[start:end] {
				numeric[i] = fixed8StringToU64(key)
				posts[i] = arena.accum(m[key]).materializeOwned()
			}
			builder.appendChunk(newNumericFieldIndexChunk(posts, numeric, postingRows(posts)))
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
				refs[i] = newFieldIndexStringRef(off, n)
				posts[i] = arena.accum(m[key]).materializeOwned()
				off += n
			}
			builder.appendChunk(newStringFieldIndexChunk(posts, refs, data, postingRows(posts)))
		}
		start = end
	}

	insertPostingMapPool.Put(m)

	return newChunkedFieldIndexStorage(builder.root())
}

func newFlatFieldIndexStorageFromFixedInsertPostingAccumsOwned(
	m map[uint64]uint32,
	arena *insertPostingAccumArena,
) fieldIndexStorage {
	if len(m) == 0 {
		fixedInsertPostingMapPool.Put(m)
		return fieldIndexStorage{}
	}
	keys := make([]uint64, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	entries := make([]index, 0, len(keys))
	for i := range keys {
		entries = append(entries, index{
			Key: indexKeyFromU64(keys[i]),
			IDs: arena.accum(m[keys[i]]).materializeOwned(),
		})
	}
	fixedInsertPostingMapPool.Put(m)
	return newFlatFieldIndexStorage(&entries)
}

func newRegularFieldIndexStorageFromFixedPostingMapOwned(m map[uint64]posting.List) fieldIndexStorage {
	if len(m) == 0 {
		fixedPostingMapPool.Put(m)
		return fieldIndexStorage{}
	}
	keys := make([]uint64, 0, len(m))
	for key, ids := range m {
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			continue
		}
		m[key] = ids
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		fixedPostingMapPool.Put(m)
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
		fixedPostingMapPool.Put(m)
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
		builder.appendChunk(newNumericFieldIndexChunk(posts, numeric, postingRows(posts)))
		start = end
	}
	fixedPostingMapPool.Put(m)
	return newChunkedFieldIndexStorage(builder.root())
}

func newRegularFieldIndexStorageFromFixedInsertPostingAccumsOwned(
	m map[uint64]uint32,
	arena *insertPostingAccumArena,
) fieldIndexStorage {
	if len(m) == 0 {
		fixedInsertPostingMapPool.Put(m)
		return fieldIndexStorage{}
	}
	keys := make([]uint64, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	slices.Sort(keys)
	if !shouldUseChunkedFieldIndex(len(keys)) {
		entries := make([]index, 0, len(keys))
		for i := range keys {
			entries = append(entries, index{
				Key: indexKeyFromU64(keys[i]),
				IDs: arena.accum(m[keys[i]]).materializeOwned(),
			})
		}
		fixedInsertPostingMapPool.Put(m)
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
			posts[i] = arena.accum(m[key]).materializeOwned()
		}
		builder.appendChunk(newNumericFieldIndexChunk(posts, numeric, postingRows(posts)))
		start = end
	}

	fixedInsertPostingMapPool.Put(m)

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
	return len(s.flat.entries)
}

func (s fieldIndexStorage) flatSlice() *[]index {
	if s.chunked != nil {
		return nil
	}
	if s.flat == nil {
		return nil
	}
	return &s.flat.entries
}

func (c *fieldIndexChunk) keyCount() int {
	if c == nil {
		return 0
	}
	if c.hasUniqueNumericOwners() {
		return len(c.numeric) >> 1
	}
	if c.hasStringKeys() {
		return len(c.stringRefs)
	}
	return len(c.numeric)
}

func (c *fieldIndexChunk) keyAt(i int) indexKey {
	if c == nil || i < 0 || i >= c.keyCount() {
		return indexKey{}
	}
	if c.hasStringKeys() {
		ref := c.stringRefs[i]
		return indexKeyFromBytes(c.stringData[int(ref.off) : int(ref.off)+int(ref.len)])
	}
	if c.hasUniqueNumericOwners() {
		return indexKeyFromU64(c.numeric[i<<1])
	}
	return indexKeyFromU64(c.numeric[i])
}

func (c *fieldIndexChunk) postingAt(i int) posting.List {
	var p posting.List
	if c == nil || i < 0 {
		return p
	}
	if c.hasUniqueStringOwners() {
		if i >= len(c.numeric) {
			return p
		}
		return p.BuildAdded(c.numeric[i])
	}
	if c.hasUniqueNumericOwners() {
		base := i << 1
		if base+1 >= len(c.numeric) {
			return p
		}
		return p.BuildAdded(c.numeric[base+1])
	}
	if i >= len(c.posts) {
		return p
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
	limit := len(c.posts)
	if c.hasUniqueStringOwners() {
		limit = len(c.numeric)
	} else if c.hasUniqueNumericOwners() {
		limit = len(c.numeric) >> 1
	}
	if start < 0 {
		start = 0
	}
	if end > limit {
		end = limit
	}
	if start >= end {
		return 0
	}
	if c.hasUniqueStringOwners() || c.hasUniqueNumericOwners() {
		return uint64(end - start)
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
			posts[i] = storedFieldPosting(entries[i].IDs)
			rows += posts[i].Cardinality()
		}
		return newNumericFieldIndexChunk(posts, keys, rows)
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
		refs[i] = newFieldIndexStringRef(off, n)
		posts[i] = storedFieldPosting(entries[i].IDs)
		rows += posts[i].Cardinality()
		off += n
	}
	return newStringFieldIndexChunk(posts, refs, data, rows)
}

func newFieldIndexChunkRefsFromEntries(entries []index) []fieldIndexChunkRef {
	if len(entries) == 0 {
		return nil
	}

	refs := make([]fieldIndexChunkRef, 0, max(1, len(entries)/fieldIndexChunkTargetEntries+1))
	numeric := entries[0].Key.isNumeric()

	for start := 0; start < len(entries); {
		size := balancedFieldIndexChunkSize(len(entries) - start)
		if !numeric {
			size = nextStringFieldIndexChunkSizeEntries(entries, start)
		}
		end := start + size
		chunk := newFieldIndexChunkFromEntries(entries[start:end])
		last := chunk.keyCount() - 1
		refs = append(refs, fieldIndexChunkRef{
			last:  chunk.keyAt(last),
			chunk: chunk,
		})
		start = end
	}

	return refs
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

func lowerBoundFieldIndexChunkKey(chunk *fieldIndexChunk, key indexKey) int {
	lo, hi := 0, chunk.keyCount()
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeys(chunk.keyAt(mid), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func lowerBoundIndexEntriesKey(entries []index, key indexKey) int {
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeys(entries[mid].Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func upperBoundIndexEntriesKey(entries []index, key indexKey) int {
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeys(entries[mid].Key, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func searchChunkPageAfterChunk(prefix *pooled.SliceBuf[int], chunk int) int {
	lo, hi := 0, prefix.Len()-1
	for lo < hi {
		mid := (lo + hi) >> 1
		if prefix.Get(mid+1) > chunk {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func searchChunkPageAtOrAfterChunk(prefix *pooled.SliceBuf[int], limit int) int {
	lo, hi := 0, prefix.Len()-1
	for lo < hi {
		mid := (lo + hi) >> 1
		if prefix.Get(mid+1) >= limit {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func searchChunkPageByLastKeyFrom(pages *pooled.SliceBuf[*fieldIndexChunkDirPage], start int, key indexKey) int {
	lo, hi := start, pages.Len()
	for lo < hi {
		mid := lo + (hi-lo)>>1
		if compareIndexKeys(pages.Get(mid).lastKey(), key) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func searchChunkRefByLastKeyFrom(page *fieldIndexChunkDirPage, start int, key indexKey) int {
	lo, hi := start, page.refsLen()
	for lo < hi {
		mid := lo + (hi-lo)>>1
		if compareIndexKeys(page.refAt(mid).last, key) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
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

func upperBoundFieldIndexChunkKey(chunk *fieldIndexChunk, key indexKey) int {
	lo, hi := 0, chunk.keyCount()
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeys(chunk.keyAt(mid), key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func lowerBoundFieldIndexChunkPrefixUpperBound(chunk *fieldIndexChunk, upper prefixUpperBound) int {
	lo, hi := 0, chunk.keyCount()
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeyPrefixUpperBound(chunk.keyAt(mid), upper) < 0 {
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

func retainedFieldIndexChunkRef(ref fieldIndexChunkRef) fieldIndexChunkRef {
	ref.chunk.retain()
	return ref
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
	if !b.numeric {
		keyLen := key.byteLen()
		if indexedStringKeyTooLongLen(keyLen) {
			panic(fmt.Sprintf("indexed string value len %d exceeds limit %d", keyLen, fieldIndexStringRefMax))
		}
		if len(b.posts) > 0 && shouldSealStringFieldIndexChunk(len(b.posts), b.stringBytes) {
			b.sealActiveChunk()
		}
	}
	b.posts = append(b.posts, ids)
	b.rows += ids.Cardinality()
	if b.numeric {
		b.numericKey = append(b.numericKey, key.meta)
	} else {
		b.stringKeys = append(b.stringKeys, key)
		b.stringBytes += key.byteLen()
	}
	if b.numeric {
		if len(b.posts) == fieldIndexChunkTargetEntries {
			b.sealActiveChunk()
		}
		return
	}
	if shouldSealStringFieldIndexChunk(len(b.posts), b.stringBytes) {
		b.sealActiveChunk()
	}
}

func (b *fieldIndexChunkStreamBuilder) publishPendingChunk() {
	if b == nil || b.builder == nil || len(b.pendingPosts) == 0 {
		return
	}
	if b.numeric {
		b.builder.appendChunk(newNumericFieldIndexChunk(b.pendingPosts, b.pendingNumericKey, b.pendingRows))
	} else {
		b.builder.appendChunk(newFieldIndexChunkFromKeys(b.pendingPosts, b.pendingStringKeys, b.pendingRows))
		b.freeStringKeys = b.pendingStringKeys[:0]
	}
	b.pendingPosts = nil
	b.pendingRows = 0
	b.pendingNumericKey = nil
	b.pendingStringKeys = nil
}

func (b *fieldIndexChunkStreamBuilder) sealActiveChunk() {
	if b == nil || b.builder == nil || len(b.posts) == 0 {
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
		if cap(b.freeStringKeys) >= fieldIndexChunkTargetEntries {
			b.stringKeys = b.freeStringKeys[:0]
			b.freeStringKeys = nil
		} else {
			b.stringKeys = make([]indexKey, 0, fieldIndexChunkTargetEntries)
		}
	}
	b.posts = make([]posting.List, 0, fieldIndexChunkTargetEntries)
	b.rows = 0
	b.stringBytes = 0
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
	for i := range posts {
		posts[i] = storedFieldPosting(posts[i])
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
		refs[i] = newFieldIndexStringRef(off, n)
		off += n
	}
	return newStringFieldIndexChunk(posts, refs, data, rows)
}

func newNumericFieldIndexChunkRefsWithInsertedEntry(ref fieldIndexChunkRef, pos int, add index) []fieldIndexChunkRef {
	chunk := ref.chunk
	if chunk == nil {
		return nil
	}
	total := chunk.keyCount() + 1
	if total <= 0 {
		return nil
	}

	if total <= fieldIndexChunkMaxEntries {
		keys := make([]uint64, total)
		posts := make([]posting.List, total)
		src := 0
		for dst := 0; dst < total; dst++ {
			if dst == pos {
				keys[dst] = add.Key.meta
				posts[dst] = add.IDs
				continue
			}
			keys[dst] = chunk.keyAt(src).meta
			posts[dst] = chunk.postingAt(src)
			src++
		}
		next := newNumericFieldIndexChunk(posts, keys, chunk.rows+add.IDs.Cardinality())
		return []fieldIndexChunkRef{{last: next.keyAt(total - 1), chunk: next}}
	}

	firstSize := balancedFieldIndexChunkSize(total)
	secondSize := total - firstSize

	firstKeys := make([]uint64, firstSize)
	firstPosts := make([]posting.List, firstSize)
	secondKeys := make([]uint64, secondSize)
	secondPosts := make([]posting.List, secondSize)

	var firstRows uint64
	var secondRows uint64

	src := 0
	for dst := 0; dst < total; dst++ {
		var key uint64
		var ids posting.List
		if dst == pos {
			key = add.Key.meta
			ids = add.IDs
		} else {
			key = chunk.keyAt(src).meta
			ids = chunk.postingAt(src)
			src++
		}
		if dst < firstSize {
			firstKeys[dst] = key
			firstPosts[dst] = ids
			firstRows += ids.Cardinality()
			continue
		}
		idx := dst - firstSize
		secondKeys[idx] = key
		secondPosts[idx] = ids
		secondRows += ids.Cardinality()
	}

	first := newNumericFieldIndexChunk(firstPosts, firstKeys, firstRows)
	second := newNumericFieldIndexChunk(secondPosts, secondKeys, secondRows)
	return []fieldIndexChunkRef{
		{last: first.keyAt(firstSize - 1), chunk: first},
		{last: second.keyAt(secondSize - 1), chunk: second},
	}
}

func newStringFieldIndexChunkRefsWithInsertedEntry(ref fieldIndexChunkRef, pos int, add index) []fieldIndexChunkRef {
	chunk := ref.chunk
	if chunk == nil {
		return nil
	}
	entries := fieldIndexChunkEntriesBorrowed(chunk)
	if len(entries) == 0 {
		return nil
	}
	entries = append(entries, index{})
	copy(entries[pos+1:], entries[pos:])
	entries[pos] = index{Key: add.Key, IDs: add.IDs}
	if len(entries) <= fieldIndexChunkMaxEntries && stringEntriesFitSingleChunk(entries) {
		next := newFieldIndexChunkFromEntries(entries)
		return []fieldIndexChunkRef{{last: next.keyAt(len(entries) - 1), chunk: next}}
	}
	return newFieldIndexChunkRefsFromEntries(entries)
}

func newFieldIndexChunkRefsWithInsertedEntry(ref fieldIndexChunkRef, pos int, add index) []fieldIndexChunkRef {
	if ref.chunk == nil {
		return nil
	}
	if ref.chunk.hasNumericKeys() {
		return newNumericFieldIndexChunkRefsWithInsertedEntry(ref, pos, add)
	}
	return newStringFieldIndexChunkRefsWithInsertedEntry(ref, pos, add)
}

func (b *fieldIndexChunkStreamBuilder) publishActiveChunk() {
	if b == nil || b.builder == nil || len(b.posts) == 0 {
		return
	}
	posts := slices.Clone(b.posts)
	if b.numeric {
		b.builder.appendChunk(newNumericFieldIndexChunk(posts, slices.Clone(b.numericKey), b.rows))
		b.numericKey = b.numericKey[:0]
	} else {
		b.builder.appendChunk(newFieldIndexChunkFromKeys(posts, b.stringKeys, b.rows))
		b.stringKeys = b.stringKeys[:0]
	}
	b.posts = b.posts[:0]
	b.rows = 0
	b.stringBytes = 0
}

func (b *fieldIndexChunkStreamBuilder) rebalancePendingWithTail() {
	if b == nil || b.builder == nil || len(b.pendingPosts) == 0 || len(b.posts) == 0 {
		return
	}
	total := len(b.pendingPosts) + len(b.posts)
	combinedPosts := make([]posting.List, 0, total)
	combinedPosts = append(combinedPosts, b.pendingPosts...)
	combinedPosts = append(combinedPosts, b.posts...)
	if b.numeric {
		firstSize := balancedFieldIndexChunkSize(total)
		secondSize := total - firstSize
		firstPosts := slices.Clone(combinedPosts[:firstSize])
		secondPosts := slices.Clone(combinedPosts[firstSize:])
		combinedKeys := make([]uint64, 0, total)
		combinedKeys = append(combinedKeys, b.pendingNumericKey...)
		combinedKeys = append(combinedKeys, b.numericKey...)
		b.builder.appendChunk(newNumericFieldIndexChunk(firstPosts, slices.Clone(combinedKeys[:firstSize]), postingRows(firstPosts)))
		b.builder.appendChunk(newNumericFieldIndexChunk(secondPosts, slices.Clone(combinedKeys[firstSize:firstSize+secondSize]), postingRows(secondPosts)))
		b.pendingNumericKey = nil
		b.numericKey = b.numericKey[:0]
	} else {
		combinedKeys := make([]indexKey, 0, total)
		combinedKeys = append(combinedKeys, b.pendingStringKeys...)
		combinedKeys = append(combinedKeys, b.stringKeys...)
		for start := 0; start < len(combinedKeys); {
			size := nextStringFieldIndexChunkSizeIndexKeys(combinedKeys, start)
			end := start + size
			b.builder.appendChunk(newFieldIndexChunkFromKeys(combinedPosts[start:end], combinedKeys[start:end], postingRows(combinedPosts[start:end])))
			start = end
		}
		b.pendingStringKeys = nil
		b.stringKeys = b.stringKeys[:0]
	}
	b.pendingPosts = nil
	b.pendingRows = 0
	b.posts = b.posts[:0]
	b.rows = 0
	b.stringBytes = 0
}

func newFieldIndexChunkBuilder(capEntries int) fieldIndexChunkBuilder {
	chunkCap := max(1, capEntries/fieldIndexChunkTargetEntries+1)
	pageCap := max(1, chunkCap/fieldIndexDirPageTargetRefs+2)
	return fieldIndexChunkBuilder{
		pages:       newFieldIndexChunkDirPageSlice(pageCap),
		pendingRefs: newFieldIndexChunkRefBuf(min(chunkCap, fieldIndexDirPageTargetRefs)),
	}
}

func (b *fieldIndexChunkBuilder) appendChunk(chunk *fieldIndexChunk) {
	if b == nil || chunk == nil || chunk.keyCount() == 0 {
		return
	}
	last := chunk.keyCount() - 1
	b.appendOwnedRef(fieldIndexChunkRef{
		last:  chunk.keyAt(last),
		chunk: chunk,
	})
}

func (b *fieldIndexChunkBuilder) appendOwnedRef(ref fieldIndexChunkRef) {
	if b == nil || ref.chunk == nil || ref.chunk.keyCount() == 0 {
		return
	}
	b.pendingRefs.Append(ref)
	size := ref.chunk.keyCount()
	b.total += size
	b.chunks++
	if b.pendingRefs.Len() >= fieldIndexDirPageTargetRefs {
		b.flushPendingPage()
	}
}

func (b *fieldIndexChunkBuilder) appendRefsRange(root *fieldIndexChunkedRoot, start, end int) {
	if b == nil || root == nil || start < 0 || end < start || end > root.chunkCount || start == end {
		return
	}
	startPage, startOff := root.pagePosForChunk(start)
	endPage, endOff := root.pagePosForChunk(end)
	if startPage == root.pages.Len() {
		return
	}
	startPageRef := root.pages.Get(startPage)
	if startPage == endPage {
		b.appendRefSlice(startPageRef, startOff, endOff)
		return
	}
	if startOff > 0 {
		b.appendRefSlice(startPageRef, startOff, startPageRef.refsLen())
		startPage++
	}
	for page := startPage; page < endPage; page++ {
		b.appendPage(root.pages.Get(page))
	}
	if endOff > 0 {
		b.appendRefSlice(root.pages.Get(endPage), 0, endOff)
	}
}

func (b *fieldIndexChunkBuilder) appendEntries(entries []index) {
	if b == nil || len(entries) == 0 {
		return
	}
	numeric := entries[0].Key.isNumeric()
	for start := 0; start < len(entries); {
		size := balancedFieldIndexChunkSize(len(entries) - start)
		if !numeric {
			size = nextStringFieldIndexChunkSizeEntries(entries, start)
		}
		end := start + size
		b.appendChunk(newFieldIndexChunkFromEntries(entries[start:end]))
		start = end
	}
}

func (b *fieldIndexChunkBuilder) appendRefSlice(page *fieldIndexChunkDirPage, start, end int) {
	if page == nil || start < 0 || end < start || end > page.refsLen() {
		return
	}
	for i := start; i < end; i++ {
		b.appendOwnedRef(retainedFieldIndexChunkRef(page.refAt(i)))
	}
}

func (b *fieldIndexChunkBuilder) flushPendingPage() {
	if b == nil || b.pendingRefs == nil || b.pendingRefs.Len() == 0 {
		return
	}
	page := newFieldIndexChunkDirPageOwned(b.pendingRefs)
	b.pages.Append(page)
	b.pendingRefs = newFieldIndexChunkRefBuf(fieldIndexDirPageTargetRefs)
}

func (b *fieldIndexChunkBuilder) appendPage(page *fieldIndexChunkDirPage) {
	if b == nil || page == nil || page.refsLen() == 0 {
		return
	}
	if page.refsLen() != fieldIndexDirPageTargetRefs || (b.pendingRefs != nil && b.pendingRefs.Len() != 0) {
		b.appendRefSlice(page, 0, page.refsLen())
		return
	}
	page.retain()
	b.pages.Append(page)
	b.total += page.keyCount()
	b.chunks += page.refsLen()
}

func (b *fieldIndexChunkBuilder) appendOwnedPage(page *fieldIndexChunkDirPage) {
	if b == nil || page == nil || page.refsLen() == 0 {
		return
	}
	if b.pendingRefs != nil && b.pendingRefs.Len() != 0 {
		b.flushPendingPage()
	}
	b.pages.Append(page)
	b.total += page.keyCount()
	b.chunks += page.refsLen()
}

func (b *fieldIndexChunkBuilder) root() *fieldIndexChunkedRoot {
	if b == nil || b.total == 0 {
		if b != nil {
			if b.pendingRefs != nil {
				fieldIndexChunkRefSlicePool.Put(b.pendingRefs)
				b.pendingRefs = nil
			}
			releaseFieldIndexChunkDirPages(b.pages)
			b.pages = nil
		}
		return nil
	}
	b.flushPendingPage()
	pages := b.pages
	b.pages = nil
	if b.pendingRefs != nil {
		fieldIndexChunkRefSlicePool.Put(b.pendingRefs)
		b.pendingRefs = nil
	}
	return newFieldIndexChunkedRootFromOwnedPages(pages)
}

func newFieldIndexChunkedRootFromPages(pages []*fieldIndexChunkDirPage) *fieldIndexChunkedRoot {
	if len(pages) == 0 {
		return nil
	}
	pageBuf := newFieldIndexChunkDirPageSlice(len(pages))
	pageBuf.AppendAll(pages)
	return newFieldIndexChunkedRootFromOwnedPages(pageBuf)
}

func newFieldIndexChunkedRootFromOwnedPages(pages *pooled.SliceBuf[*fieldIndexChunkDirPage]) *fieldIndexChunkedRoot {
	if pages == nil || pages.Len() == 0 {
		return nil
	}
	chunkPrefix := fieldIndexChunkRootChunkPrefixPool.Get()
	chunkPrefix.SetLen(pages.Len() + 1)
	chunkPrefix.Set(0, 0)
	prefix := fieldIndexChunkRootPrefixPool.Get()
	prefix.SetLen(pages.Len() + 1)
	prefix.Set(0, 0)
	rowPrefix := fieldIndexChunkRootRowPrefixPool.Get()
	rowPrefix.SetLen(pages.Len() + 1)
	rowPrefix.Set(0, 0)

	chunks := 0
	total := 0
	rows := uint64(0)

	for i := 0; i < pages.Len(); i++ {
		page := pages.Get(i)
		chunks += page.refsLen()
		total += page.keyCount()
		rows += page.rowPrefixAt(page.refsLen())
		chunkPrefix.Set(i+1, chunks)
		prefix.Set(i+1, total)
		rowPrefix.Set(i+1, rows)
	}
	if total == 0 {
		releaseFieldIndexChunkDirPages(pages)
		return nil
	}
	root := &fieldIndexChunkedRoot{
		pages:       pages,
		chunkPrefix: chunkPrefix,
		prefix:      prefix,
		rowPrefix:   rowPrefix,
		keyCount:    total,
		chunkCount:  chunks,
	}
	root.refs.Store(1)
	return root
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
	for i := 0; i < r.pages.Len(); i++ {
		page := r.pages.Get(i)
		for j := 0; j < page.refsLen(); j++ {
			chunk := page.refAt(j).chunk
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
	if r == nil || r.pages == nil || r.pages.Len() == 0 {
		return 0, 0
	}
	if chunk < 0 || chunk >= r.chunkCount {
		return r.pages.Len(), 0
	}
	page := searchChunkPageAfterChunk(r.chunkPrefix, chunk)
	if page >= r.pages.Len() {
		return r.pages.Len(), 0
	}
	return page, chunk - r.chunkPrefix.Get(page)
}

func (r *fieldIndexChunkedRoot) refAtChunk(chunk int) (fieldIndexChunkRef, bool) {
	page, off := r.pagePosForChunk(chunk)
	if r.pages == nil || page >= r.pages.Len() {
		return fieldIndexChunkRef{}, false
	}
	return r.pages.Get(page).refAt(off), true
}

func (r *fieldIndexChunkedRoot) entryPrefixForChunk(limit int) int {
	if r == nil || limit <= 0 {
		return 0
	}
	if limit >= r.chunkCount {
		return r.keyCount
	}
	page := searchChunkPageAtOrAfterChunk(r.chunkPrefix, limit)
	if r.pages == nil || page >= r.pages.Len() {
		return r.keyCount
	}
	off := limit - r.chunkPrefix.Get(page)
	return r.prefix.Get(page) + r.pages.Get(page).prefixAt(off)
}

func (r *fieldIndexChunkedRoot) rowPrefixForChunk(limit int) uint64 {
	if r == nil || limit <= 0 {
		return 0
	}
	if limit >= r.chunkCount {
		if r.rowPrefix == nil || r.rowPrefix.Len() == 0 {
			return 0
		}
		return r.rowPrefix.Get(r.rowPrefix.Len() - 1)
	}
	page := searchChunkPageAtOrAfterChunk(r.chunkPrefix, limit)
	if r.pages == nil || page >= r.pages.Len() {
		if r.rowPrefix == nil || r.rowPrefix.Len() == 0 {
			return 0
		}
		return r.rowPrefix.Get(r.rowPrefix.Len() - 1)
	}
	off := limit - r.chunkPrefix.Get(page)
	return r.rowPrefix.Get(page) + r.pages.Get(page).rowPrefixAt(off)
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
	page := sort.Search(r.pages.Len(), func(i int) bool {
		return r.prefix.Get(i+1) > rank
	})
	if page >= r.pages.Len() {
		return r.endPos()
	}
	pageRef := r.pages.Get(page)
	localRank := rank - r.prefix.Get(page)
	refIdx := sort.Search(pageRef.refsLen(), func(i int) bool {
		return pageRef.prefixAt(i+1) > localRank
	})
	if refIdx >= pageRef.refsLen() {
		return r.endPos()
	}
	return fieldIndexChunkPos{
		chunk: r.chunkPrefix.Get(page) + refIdx,
		entry: localRank - pageRef.prefixAt(refIdx),
	}
}

func (r *fieldIndexChunkedRoot) lowerBoundPos(key string) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, 0
	}
	page := sort.Search(r.pages.Len(), func(i int) bool {
		return compareIndexKeyString(r.pages.Get(i).lastKey(), key) >= 0
	})
	if page >= r.pages.Len() {
		return r.endPos(), r.keyCount
	}
	pageRef := r.pages.Get(page)
	refIdx := sort.Search(pageRef.refsLen(), func(i int) bool {
		return compareIndexKeyString(pageRef.refAt(i).last, key) >= 0
	})
	if refIdx >= pageRef.refsLen() {
		return r.endPos(), r.keyCount
	}
	ref := pageRef.refAt(refIdx)
	chunk := ref.chunk
	entryIdx := lowerBoundFieldIndexChunk(chunk, key)
	rank := r.prefix.Get(page) + pageRef.prefixAt(refIdx) + entryIdx
	if entryIdx >= chunk.keyCount() {
		nextChunk := r.chunkPrefix.Get(page) + refIdx + 1
		if nextChunk >= r.chunkCount {
			return r.endPos(), r.keyCount
		}
		return fieldIndexChunkPos{chunk: nextChunk}, rank
	}
	return fieldIndexChunkPos{chunk: r.chunkPrefix.Get(page) + refIdx, entry: entryIdx}, rank
}

func (r *fieldIndexChunkedRoot) upperBoundPos(key string) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, 0
	}
	page := sort.Search(r.pages.Len(), func(i int) bool {
		return compareIndexKeyString(r.pages.Get(i).lastKey(), key) > 0
	})
	if page >= r.pages.Len() {
		return r.endPos(), r.keyCount
	}
	pageRef := r.pages.Get(page)
	refIdx := sort.Search(pageRef.refsLen(), func(i int) bool {
		return compareIndexKeyString(pageRef.refAt(i).last, key) > 0
	})
	if refIdx >= pageRef.refsLen() {
		return r.endPos(), r.keyCount
	}
	ref := pageRef.refAt(refIdx)
	chunk := ref.chunk
	entryIdx := upperBoundFieldIndexChunk(chunk, key)
	rank := r.prefix.Get(page) + pageRef.prefixAt(refIdx) + entryIdx
	if entryIdx >= chunk.keyCount() {
		nextChunk := r.chunkPrefix.Get(page) + refIdx + 1
		if nextChunk >= r.chunkCount {
			return r.endPos(), r.keyCount
		}
		return fieldIndexChunkPos{chunk: nextChunk}, rank
	}
	return fieldIndexChunkPos{chunk: r.chunkPrefix.Get(page) + refIdx, entry: entryIdx}, rank
}

func (r *fieldIndexChunkedRoot) lowerBoundPosKey(key indexKey) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, 0
	}
	page := sort.Search(r.pages.Len(), func(i int) bool {
		return compareIndexKeys(r.pages.Get(i).lastKey(), key) >= 0
	})
	if page >= r.pages.Len() {
		return r.endPos(), r.keyCount
	}
	pageRef := r.pages.Get(page)
	refIdx := sort.Search(pageRef.refsLen(), func(i int) bool {
		return compareIndexKeys(pageRef.refAt(i).last, key) >= 0
	})
	if refIdx >= pageRef.refsLen() {
		return r.endPos(), r.keyCount
	}
	ref := pageRef.refAt(refIdx)
	chunk := ref.chunk
	entryIdx := lowerBoundFieldIndexChunkKey(chunk, key)
	rank := r.prefix.Get(page) + pageRef.prefixAt(refIdx) + entryIdx
	if entryIdx >= chunk.keyCount() {
		nextChunk := r.chunkPrefix.Get(page) + refIdx + 1
		if nextChunk >= r.chunkCount {
			return r.endPos(), r.keyCount
		}
		return fieldIndexChunkPos{chunk: nextChunk}, rank
	}
	return fieldIndexChunkPos{chunk: r.chunkPrefix.Get(page) + refIdx, entry: entryIdx}, rank
}

func (r *fieldIndexChunkedRoot) upperBoundPosKey(key indexKey) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, 0
	}
	page := sort.Search(r.pages.Len(), func(i int) bool {
		return compareIndexKeys(r.pages.Get(i).lastKey(), key) > 0
	})
	if page >= r.pages.Len() {
		return r.endPos(), r.keyCount
	}
	pageRef := r.pages.Get(page)
	refIdx := sort.Search(pageRef.refsLen(), func(i int) bool {
		return compareIndexKeys(pageRef.refAt(i).last, key) > 0
	})
	if refIdx >= pageRef.refsLen() {
		return r.endPos(), r.keyCount
	}
	ref := pageRef.refAt(refIdx)
	chunk := ref.chunk
	entryIdx := upperBoundFieldIndexChunkKey(chunk, key)
	rank := r.prefix.Get(page) + pageRef.prefixAt(refIdx) + entryIdx
	if entryIdx >= chunk.keyCount() {
		nextChunk := r.chunkPrefix.Get(page) + refIdx + 1
		if nextChunk >= r.chunkCount {
			return r.endPos(), r.keyCount
		}
		return fieldIndexChunkPos{chunk: nextChunk}, rank
	}
	return fieldIndexChunkPos{chunk: r.chunkPrefix.Get(page) + refIdx, entry: entryIdx}, rank
}

func (r *fieldIndexChunkedRoot) lowerBoundPosPrefixUpperBound(upper prefixUpperBound) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, 0
	}
	page := sort.Search(r.pages.Len(), func(i int) bool {
		return compareIndexKeyPrefixUpperBound(r.pages.Get(i).lastKey(), upper) >= 0
	})
	if page >= r.pages.Len() {
		return r.endPos(), r.keyCount
	}
	pageRef := r.pages.Get(page)
	refIdx := sort.Search(pageRef.refsLen(), func(i int) bool {
		return compareIndexKeyPrefixUpperBound(pageRef.refAt(i).last, upper) >= 0
	})
	if refIdx >= pageRef.refsLen() {
		return r.endPos(), r.keyCount
	}
	ref := pageRef.refAt(refIdx)
	chunk := ref.chunk
	entryIdx := lowerBoundFieldIndexChunkPrefixUpperBound(chunk, upper)
	rank := r.prefix.Get(page) + pageRef.prefixAt(refIdx) + entryIdx
	if entryIdx >= chunk.keyCount() {
		nextChunk := r.chunkPrefix.Get(page) + refIdx + 1
		if nextChunk >= r.chunkCount {
			return r.endPos(), r.keyCount
		}
		return fieldIndexChunkPos{chunk: nextChunk}, rank
	}
	return fieldIndexChunkPos{chunk: r.chunkPrefix.Get(page) + refIdx, entry: entryIdx}, rank
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
	upper, ok := newPrefixUpperBound(prefix)
	if !ok {
		return r.endPos(), r.keyCount
	}
	return r.lowerBoundPosPrefixUpperBound(upper)
}

func (r *fieldIndexChunkedRoot) lookupPostingRetained(key string) posting.List {
	if r == nil || r.chunkCount == 0 {
		return posting.List{}
	}
	page := sort.Search(r.pages.Len(), func(i int) bool {
		return compareIndexKeyString(r.pages.Get(i).lastKey(), key) >= 0
	})
	if page >= r.pages.Len() {
		return posting.List{}
	}
	pageRef := r.pages.Get(page)
	refIdx := sort.Search(pageRef.refsLen(), func(i int) bool {
		return compareIndexKeyString(pageRef.refAt(i).last, key) >= 0
	})
	if refIdx >= pageRef.refsLen() {
		return posting.List{}
	}
	ref := pageRef.refAt(refIdx)
	chunk := ref.chunk
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
	page := searchChunkPageByLastKeyFrom(r.pages, startPage, key)
	if r.pages == nil || page >= r.pages.Len() {
		return r.chunkCount - 1
	}
	pageRef := r.pages.Get(page)
	refStart := 0
	if page == startPage {
		refStart = startOff
	}
	refIdx := searchChunkRefByLastKeyFrom(pageRef, refStart, key)
	if refIdx >= pageRef.refsLen() {
		return r.chunkPrefix.Get(page+1) - 1
	}
	return r.chunkPrefix.Get(page) + refIdx
}
