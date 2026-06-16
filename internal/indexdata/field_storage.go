package indexdata

import (
	"fmt"
	"slices"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

const (
	fieldIndexChunkTargetEntries = 192
	fieldIndexChunkMinEntries    = fieldIndexChunkTargetEntries / 2
	fieldIndexChunkMaxEntries    = fieldIndexChunkTargetEntries + fieldIndexChunkMinEntries
	fieldIndexChunkThreshold     = fieldIndexChunkTargetEntries * 2
	fieldIndexDirPageTargetRefs  = fieldIndexChunkTargetEntries
	fieldIndexStringRefMax       = 1<<16 - 1

	FieldChunkTargetEntries = fieldIndexChunkTargetEntries
	FieldChunkMaxEntries    = fieldIndexChunkMaxEntries
	FieldChunkThreshold     = fieldIndexChunkThreshold
	FieldDirPageTargetRefs  = fieldIndexDirPageTargetRefs
	FieldStringRefMax       = fieldIndexStringRefMax

	NilIndexEntryKey = ""
)

type Entry struct {
	Key keycodec.IndexKey
	IDs posting.List
}

type fieldIndexStringRef = uint32

func ValidateIndexedStringKeyLen(n int) error {
	if n > fieldIndexStringRefMax {
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
		return fieldIndexStringRef(0)
	}
	if off > fieldIndexStringRefMax {
		panic("field Entry string ref offset exceeds uint16")
	}
	if n > fieldIndexStringRefMax {
		panic("field Entry string ref len exceeds uint16")
	}
	return uint32(off)<<16 | uint32(n)
}

func fieldIndexStringRefOff(ref fieldIndexStringRef) int {
	return int(ref >> 16)
}

func fieldIndexStringRefLen(ref fieldIndexStringRef) int {
	return int(ref & fieldIndexStringRefMax)
}

func nextStringFieldIndexChunkSizeStrings(keys []string, start int) int {
	target := balancedFieldIndexChunkSize(len(keys) - start)
	size := 0
	bytes := 0
	limit := min(len(keys)-start, fieldIndexChunkTargetEntries)

	for i := start; size < limit; i++ {
		keyLen := len(keys[i])
		if keyLen > fieldIndexStringRefMax {
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

func nextStringFieldIndexChunkSizeIndexKeys(keys []keycodec.IndexKey, start int) int {
	target := balancedFieldIndexChunkSize(len(keys) - start)
	size := 0
	bytes := 0
	limit := min(len(keys)-start, fieldIndexChunkTargetEntries)

	for i := start; size < limit; i++ {
		keyLen := keys[i].ByteLen()
		if keyLen > fieldIndexStringRefMax {
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

func nextStringFieldIndexChunkSizeEntries(entries []Entry, start int) int {
	target := balancedFieldIndexChunkSize(len(entries) - start)
	size := 0
	bytes := 0
	limit := min(len(entries)-start, fieldIndexChunkTargetEntries)

	for i := start; size < limit; i++ {
		keyLen := entries[i].Key.ByteLen()
		if keyLen > fieldIndexStringRefMax {
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

func stringEntriesFitSingleChunk(entries []Entry) bool {
	bytes := 0
	for i := range entries {
		keyLen := entries[i].Key.ByteLen()
		if keyLen > fieldIndexStringRefMax {
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
	refs       atomic.Int32
	entries    []Entry
	stringData []byte
}

type fieldIndexChunk struct {
	refs       atomic.Int32
	posts      []posting.List
	numeric    []uint64
	stringRefs []fieldIndexStringRef
	stringData []byte
	rows       uint64
}

func (chunk *fieldIndexChunk) hasStringKeys() bool {
	return chunk != nil && chunk.stringRefs != nil
}

func (chunk *fieldIndexChunk) hasNumericKeys() bool {
	return chunk != nil && chunk.stringRefs == nil && chunk.numeric != nil
}

func (chunk *fieldIndexChunk) hasUniqueStringOwners() bool {
	return chunk != nil && chunk.stringRefs != nil && chunk.numeric != nil && chunk.posts == nil
}

func (chunk *fieldIndexChunk) hasUniqueNumericOwners() bool {
	return chunk != nil && chunk.stringRefs == nil && chunk.numeric != nil && chunk.posts == nil
}

type fieldIndexChunkRef struct {
	last  keycodec.IndexKey
	chunk *fieldIndexChunk
}

type fieldIndexChunkDirPage struct {
	refsCount atomic.Int32
	refs      []fieldIndexChunkRef
	prefix    []int
	rowPrefix []uint64
}

// fieldIndexChunkedRoot stores immutable directory pages over immutable chunks
// plus top-level chunk/entry prefixes for fast rank/span calculations.
type fieldIndexChunkedRoot struct {
	refs        atomic.Int32
	pages       []*fieldIndexChunkDirPage
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

// FieldStorage is a compact nil-based union for one field Entry backend.
type FieldStorage struct {
	flat    *fieldIndexFlatRoot
	chunked *fieldIndexChunkedRoot
}

func FieldStorageSlotsApproxBytes(slots []FieldStorage) uint64 {
	return uint64(cap(slots)) * uint64(unsafe.Sizeof(FieldStorage{}))
}

type fieldIndexChunkBuilder struct {
	pages       []*fieldIndexChunkDirPage
	pendingRefs []fieldIndexChunkRef
	total       int
	chunks      int
}

type fieldIndexChunkStreamBuilder struct {
	numeric           bool
	posts             []posting.List
	rows              uint64
	stringBytes       int
	numericKey        []uint64
	stringKeys        []keycodec.IndexKey
	freeStringKeys    []keycodec.IndexKey
	pendingPosts      []posting.List
	pendingRows       uint64
	pendingNumericKey []uint64
	pendingStringKeys []keycodec.IndexKey
}

type FieldStorageRun struct {
	stringBuf []string
	u64Buf    []uint64
	postBuf   []posting.List
}

type fieldStorageRunCursor struct {
	run int
	pos int
	key keycodec.IndexKey
}

type fieldStorageRunHeap struct {
	runs []FieldStorageRun
	buf  []fieldStorageRunCursor
}

func fieldPostingSlice(n int) []posting.List          { return posting.GetSlice(n)[:n] }
func fieldUint64Slice(n int) []uint64                 { return pooled.GetUint64Slice(n)[:n] }
func fieldStringRefSlice(n int) []fieldIndexStringRef { return pooled.GetUint32Slice(n)[:n] }
func fieldByteSlice(n int) []byte                     { return pooled.GetByteSlice(n)[:n] }

func newFieldIndexChunkDirPage(refs []fieldIndexChunkRef) *fieldIndexChunkDirPage {
	if len(refs) == 0 {
		return nil
	}
	page := fieldIndexChunkDirPagePool.Get()
	page.refsCount.Store(1)
	if cap(page.refs) < len(refs) {
		page.refs = make([]fieldIndexChunkRef, len(refs))
	} else {
		page.refs = page.refs[:len(refs)]
	}
	copy(page.refs, refs)

	prefixLen := len(refs) + 1
	if cap(page.prefix) < prefixLen {
		page.prefix = make([]int, prefixLen)
	} else {
		page.prefix = page.prefix[:prefixLen]
	}
	page.prefix[0] = 0
	if cap(page.rowPrefix) < prefixLen {
		page.rowPrefix = make([]uint64, prefixLen)
	} else {
		page.rowPrefix = page.rowPrefix[:prefixLen]
	}
	page.rowPrefix[0] = 0

	total := 0
	rows := uint64(0)
	for i := range page.refs {
		ref := page.refs[i]
		total += ref.chunk.keyCount()
		rows += ref.chunk.rowCount()
		page.prefix[i+1] = total
		page.rowPrefix[i+1] = rows
	}
	return page
}

func newFieldIndexChunkDirPageOwned(refs []fieldIndexChunkRef) *fieldIndexChunkDirPage {
	page := newFieldIndexChunkDirPage(refs)
	fieldIndexChunkRefSlicePool.Put(refs)
	return page
}

func (p *fieldIndexChunkDirPage) retain() {
	p.refsCount.Add(1)
}

func (p *fieldIndexChunkDirPage) release() {
	if p == nil || p.refsCount.Add(-1) != 0 {
		return
	}
	fieldIndexChunkDirPagePool.Put(p)
}

func (p *fieldIndexChunkDirPage) keyCount() int {
	if len(p.prefix) == 0 {
		return 0
	}
	return p.prefix[len(p.prefix)-1]
}

func (p *fieldIndexChunkDirPage) lastKey() keycodec.IndexKey {
	if len(p.refs) == 0 {
		return keycodec.IndexKey{}
	}
	return p.refs[len(p.refs)-1].last
}

func releaseFieldIndexChunkDirPages(pages []*fieldIndexChunkDirPage) {
	if pages == nil {
		return
	}
	for i := range pages {
		page := pages[i]
		if page != nil {
			page.release()
		}
	}
	fieldIndexChunkDirPageSlicePool.Put(pages)
}

func storedFieldPosting(ids posting.List) posting.List {
	if ids.IsBorrowed() {
		return ids.Clone()
	}
	return ids
}

func (entry Entry) borrow() Entry {
	entry.IDs = entry.IDs.Borrow()
	return entry
}

func copyBorrowedIndexEntries(dst, src []Entry) {
	for i := range src {
		dst[i] = src[i].borrow()
	}
}

func appendBorrowedIndexEntries(dst []Entry, src []Entry) []Entry {
	for i := range src {
		dst = append(dst, src[i].borrow())
	}
	return dst
}

func newNumericFieldIndexChunk(posts []posting.List, keys []uint64, rows uint64) *fieldIndexChunk {
	if len(posts) > 0 {
		if owner, ok := posts[0].TrySingle(); ok {
			keyOwners := fieldUint64Slice(len(posts) * 2)
			keyOwners[0] = keys[0]
			keyOwners[1] = owner
			for i := 1; i < len(posts); i++ {
				owner, ok = posts[i].TrySingle()
				if !ok {
					pooled.ReleaseUint64Slice(keyOwners)
					goto REGULAR
				}
				base := i << 1
				keyOwners[base] = keys[i]
				keyOwners[base+1] = owner
			}
			posting.ReleaseAll(posts)
			posting.ReleaseSlice(posts)
			pooled.ReleaseUint64Slice(keys)
			return newUniqueNumericFieldIndexChunk(keyOwners)
		}
	}
REGULAR:
	for i := range posts {
		posts[i] = storedFieldPosting(posts[i])
	}
	chunk := fieldIndexChunkPool.Get()
	chunk.posts = posts
	chunk.numeric = keys
	chunk.rows = rows
	chunk.refs.Store(1)
	return chunk
}

func newUniqueNumericFieldIndexChunk(keyOwners []uint64) *fieldIndexChunk {
	if len(keyOwners)&1 != 0 {
		panic("numeric owner chunk key/owner data len mismatch")
	}
	chunk := fieldIndexChunkPool.Get()
	chunk.numeric = keyOwners
	chunk.rows = uint64(len(keyOwners) >> 1)
	chunk.refs.Store(1)
	return chunk
}

func newUniqueStringFieldIndexChunk(owners []uint64, refs []fieldIndexStringRef, data []byte) *fieldIndexChunk {
	if len(owners) != len(refs) {
		panic("string owner chunk owners/refs len mismatch")
	}
	chunk := fieldIndexChunkPool.Get()
	chunk.numeric = owners
	chunk.stringRefs = refs
	chunk.stringData = data
	chunk.rows = uint64(len(refs))
	chunk.refs.Store(1)
	return chunk
}

func newStringFieldIndexChunk(posts []posting.List, refs []fieldIndexStringRef, data []byte, rows uint64) *fieldIndexChunk {
	if len(posts) > 0 {
		if owner, ok := posts[0].TrySingle(); ok {
			owners := fieldUint64Slice(len(posts))
			owners[0] = owner
			for i := 1; i < len(posts); i++ {
				owner, ok = posts[i].TrySingle()
				if !ok {
					pooled.ReleaseUint64Slice(owners)
					goto REGULAR
				}
				owners[i] = owner
			}
			posting.ReleaseAll(posts)
			posting.ReleaseSlice(posts)
			return newUniqueStringFieldIndexChunk(owners, refs, data)
		}
	}
REGULAR:
	for i := range posts {
		posts[i] = storedFieldPosting(posts[i])
	}
	chunk := fieldIndexChunkPool.Get()
	chunk.posts = posts
	chunk.stringRefs = refs
	chunk.stringData = data
	chunk.rows = rows
	chunk.refs.Store(1)
	return chunk
}

func newFlatFieldStorage(entries []Entry, stringData []byte) FieldStorage {
	if len(entries) == 0 {
		pooled.ReleaseByteSlice(stringData)
		return FieldStorage{}
	}
	if stringData == nil && !entries[0].Key.IsNumeric() {
		totalBytes := 0
		for i := range entries {
			if entries[i].Key.IsNumeric() {
				continue
			}
			n := entries[i].Key.ByteLen()
			if n > fieldIndexStringRefMax {
				panic("field Entry string key len exceeds uint16")
			}
			totalBytes += n
		}
		if totalBytes > 0 {
			stringData = fieldByteSlice(totalBytes)
			off := 0
			for i := range entries {
				if entries[i].Key.IsNumeric() {
					continue
				}
				n := copy(stringData[off:], entries[i].Key.UnsafeString())
				entries[i].Key = keycodec.FromBytes(stringData[off : off+n])
				off += n
			}
		}
	}
	root := fieldIndexFlatRootPool.Get()
	root.entries = entries
	root.stringData = stringData
	for i := range root.entries {
		root.entries[i].IDs = storedFieldPosting(root.entries[i].IDs)
	}
	root.refs.Store(1)
	return FieldStorage{flat: root}
}

func newChunkedFieldStorage(root *fieldIndexChunkedRoot) FieldStorage {
	if root == nil || root.keyCount == 0 {
		return FieldStorage{}
	}
	return FieldStorage{chunked: root}
}

type fieldStorageBuilder struct {
	builder fieldIndexChunkBuilder
	stream  fieldIndexChunkStreamBuilder
}

func (b *fieldStorageBuilder) init(capEntries int, numeric bool) {
	b.builder = newFieldIndexChunkBuilder(capEntries)
	b.stream = newFieldIndexChunkStreamBuilder(numeric)
}

func (b *fieldStorageBuilder) append(key keycodec.IndexKey, ids posting.List) {
	b.stream.append(&b.builder, key, ids)
}

func (b *fieldStorageBuilder) finish() FieldStorage {
	b.stream.finish(&b.builder)
	return newChunkedFieldStorage(b.builder.root())
}

func (b *fieldStorageBuilder) releaseOwned() {
	b.stream.releaseOwned()
	b.builder.releaseOwned()
}

type SortedUniqueStringFieldStorageBuilder struct {
	entries []Entry
	builder fieldStorageBuilder
	capHint int
	chunked bool
}

func (b *SortedUniqueStringFieldStorageBuilder) Init(capHint int) {
	b.capHint = capHint
	entryCap := min(max(capHint, 1), fieldIndexChunkThreshold)
	b.entries = GetFieldEntrySlice(entryCap)
}

func (b *SortedUniqueStringFieldStorageBuilder) AppendBytes(key []byte, id uint64) {
	ids := (posting.List{}).BuildAdded(id)
	if b.chunked {
		b.builder.append(keycodec.FromBytes(key), ids)
		return
	}
	b.entries = append(b.entries, Entry{Key: keycodec.FromBytes(key), IDs: ids})
	if len(b.entries) < fieldIndexChunkThreshold {
		return
	}
	b.builder.init(max(b.capHint, len(b.entries)), false)
	for i := range b.entries {
		b.builder.append(b.entries[i].Key, b.entries[i].IDs)
	}
	ReleaseFieldEntrySlice(b.entries)
	b.entries = nil
	b.chunked = true
}

func (b *SortedUniqueStringFieldStorageBuilder) Finish() FieldStorage {
	if b.chunked {
		storage := b.builder.finish()
		*b = SortedUniqueStringFieldStorageBuilder{}
		return storage
	}
	if len(b.entries) == 0 {
		ReleaseFieldEntrySlice(b.entries)
		*b = SortedUniqueStringFieldStorageBuilder{}
		return FieldStorage{}
	}
	storage := newFlatFieldStorage(b.entries, nil)
	*b = SortedUniqueStringFieldStorageBuilder{}
	return storage
}

func (b *SortedUniqueStringFieldStorageBuilder) Release() {
	if b.entries != nil {
		for i := range b.entries {
			b.entries[i].IDs.Release()
		}
		ReleaseFieldEntrySlice(b.entries)
	}
	b.builder.releaseOwned()
	*b = SortedUniqueStringFieldStorageBuilder{}
}

func newRegularFieldStorage(entries []Entry) FieldStorage {
	if len(entries) == 0 {
		return FieldStorage{}
	}
	if len(entries) < fieldIndexChunkThreshold {
		return newFlatFieldStorage(entries, nil)
	}
	return newChunkedFieldStorage(buildChunkedFieldIndexRoot(entries))
}

func (r *fieldIndexFlatRoot) retain() {
	r.refs.Add(1)
}

func (r *fieldIndexFlatRoot) release() {
	if r == nil || r.refs.Add(-1) != 0 {
		return
	}
	fieldIndexFlatRootPool.Put(r)
}

func (chunk *fieldIndexChunk) retain() {
	chunk.refs.Add(1)
}

func (chunk *fieldIndexChunk) release() {
	if chunk == nil || chunk.refs.Add(-1) != 0 {
		return
	}
	fieldIndexChunkPool.Put(chunk)
}

func (r *fieldIndexChunkedRoot) retain() {
	r.refs.Add(1)
}

func (r *fieldIndexChunkedRoot) release() {
	if r == nil || r.refs.Add(-1) != 0 {
		return
	}
	fieldIndexChunkedRootPool.Put(r)
}

func (s FieldStorage) retain() {
	if s.flat != nil {
		s.flat.retain()
		return
	}
	if s.chunked != nil {
		s.chunked.retain()
	}
}

func (s FieldStorage) Release() {
	if s.flat != nil {
		s.flat.release()
		return
	}
	if s.chunked != nil {
		s.chunked.release()
	}
}

func ReleaseFieldStorageMap(m map[string]FieldStorage) {
	if m == nil {
		return
	}
	for _, storage := range m {
		storage.Release()
	}
}

func RetainSharedFieldStorageSlots(next, prev []FieldStorage) {
	if next == nil || prev == nil {
		return
	}
	limit := min(len(next), len(prev))
	for i := 0; i < limit; i++ {
		storage := next[i]
		if storage == prev[i] {
			storage.retain()
		}
	}
}

func RetainSharedFieldStorage(next, prev FieldStorage) {
	if next == prev {
		next.retain()
	}
}

func CloneFieldStorageSlots(src []FieldStorage, size int) []FieldStorage {
	out := GetFieldStorageSlice(size)[:size]
	if src == nil {
		return out
	}
	limit := min(size, len(src))
	for i := 0; i < limit; i++ {
		out[i] = src[i]
	}
	return out
}

func ReleaseFieldStorageSlots(slots []FieldStorage) {
	if slots == nil {
		return
	}
	for i := range slots {
		slots[i].Release()
	}
	ReleaseFieldStorageSlice(slots)
}

func (r FieldStorageRun) KeyCount() int {
	if r.u64Buf != nil {
		return len(r.u64Buf)
	}
	if r.stringBuf == nil {
		return 0
	}
	return len(r.stringBuf)
}

func (r FieldStorageRun) keyAt(i int) keycodec.IndexKey {
	if r.u64Buf != nil {
		return keycodec.FromU64(r.u64Buf[i])
	}
	return keycodec.FromString(r.stringBuf[i])
}

func (r *FieldStorageRun) takePosting(i int) posting.List {
	ids := r.postBuf[i]
	r.postBuf[i] = posting.List{}
	return ids
}

func (r *FieldStorageRun) ReleaseOwned() {
	if r.stringBuf != nil {
		pooled.ReleaseStringSlice(r.stringBuf)
		r.stringBuf = nil
	}
	if r.u64Buf != nil {
		pooled.ReleaseUint64Slice(r.u64Buf)
		r.u64Buf = nil
	}
	if r.postBuf != nil {
		posting.ReleaseAll(r.postBuf)
		posting.ReleaseSlice(r.postBuf)
		r.postBuf = nil
	}
}

func ReleaseFieldStorageRunsOwned(runs []FieldStorageRun) {
	for i := range runs {
		runs[i].ReleaseOwned()
	}
}

func NewStringFieldStorageRunFromPostingMap(m map[string]posting.List) FieldStorageRun {
	if len(m) == 0 {
		return FieldStorageRun{}
	}
	keyBuf := pooled.GetStringSlice(len(m))
	for key, ids := range m {
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			delete(m, key)
			continue
		}
		m[key] = ids
		keyBuf = append(keyBuf, key)
	}
	if len(keyBuf) == 0 {
		pooled.ReleaseStringSlice(keyBuf)
		return FieldStorageRun{}
	}
	sort.Strings(keyBuf)
	postBuf := posting.GetSlice(len(keyBuf))[:len(keyBuf)]
	for i := 0; i < len(keyBuf); i++ {
		postBuf[i] = m[keyBuf[i]]
	}
	clear(m)
	return FieldStorageRun{
		stringBuf: keyBuf,
		postBuf:   postBuf,
	}
}

func NewFixedFieldStorageRunFromPostingMap(m map[uint64]posting.List) FieldStorageRun {
	if len(m) == 0 {
		return FieldStorageRun{}
	}
	keyBuf := pooled.GetUint64Slice(len(m))
	for key, ids := range m {
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			delete(m, key)
			continue
		}
		m[key] = ids
		keyBuf = append(keyBuf, key)
	}
	if len(keyBuf) == 0 {
		pooled.ReleaseUint64Slice(keyBuf)
		return FieldStorageRun{}
	}
	slices.Sort(keyBuf)
	postBuf := posting.GetSlice(len(keyBuf))[:len(keyBuf)]
	for i := 0; i < len(keyBuf); i++ {
		postBuf[i] = m[keyBuf[i]]
	}
	clear(m)
	return FieldStorageRun{
		u64Buf:  keyBuf,
		postBuf: postBuf,
	}
}

func (h fieldStorageRunHeap) Len() int { return len(h.buf) }

func (h fieldStorageRunHeap) less(i, j int) bool {
	return keycodec.Compare(h.buf[i].key, h.buf[j].key) < 0
}

func (h fieldStorageRunHeap) swap(i, j int) {
	h.buf[i], h.buf[j] = h.buf[j], h.buf[i]
}

func (h *fieldStorageRunHeap) init() {
	for i := len(h.buf)/2 - 1; i >= 0; i-- {
		h.down(i)
	}
}

func (h *fieldStorageRunHeap) up(pos int) {
	for pos > 0 {
		parent := (pos - 1) >> 1
		if !h.less(pos, parent) {
			return
		}
		h.swap(parent, pos)
		pos = parent
	}
}

func (h *fieldStorageRunHeap) down(pos int) {
	for {
		left := pos*2 + 1
		if left >= len(h.buf) {
			return
		}
		smallest := left
		right := left + 1
		if right < len(h.buf) && h.less(right, left) {
			smallest = right
		}
		if !h.less(smallest, pos) {
			return
		}
		h.swap(pos, smallest)
		pos = smallest
	}
}

func (h *fieldStorageRunHeap) push(run, pos int) {
	cur := h.runs[run]
	if pos < 0 || pos >= cur.KeyCount() {
		return
	}
	h.buf = append(h.buf, fieldStorageRunCursor{
		run: run,
		pos: pos,
		key: cur.keyAt(pos),
	})
	h.up(len(h.buf) - 1)
}

func (h *fieldStorageRunHeap) pop() fieldStorageRunCursor {
	item := h.buf[0]
	last := len(h.buf) - 1
	h.buf[0] = h.buf[last]
	h.buf = h.buf[:last]
	if len(h.buf) > 0 {
		h.down(0)
	}
	return item
}

func NewRegularFieldStorageFromRunsOwned(runs []FieldStorageRun) FieldStorage {
	if len(runs) == 0 {
		return FieldStorage{}
	}

	total := 0
	numeric := false
	for i := range runs {
		if runs[i].KeyCount() == 0 {
			continue
		}
		if total == 0 {
			numeric = runs[i].u64Buf != nil
		}
		total += runs[i].KeyCount()
	}
	if total == 0 {
		ReleaseFieldStorageRunsOwned(runs)
		return FieldStorage{}
	}

	h := fieldStorageRunHeap{
		runs: runs,
		buf:  fieldStorageRunCursorSlicePool.Get(len(runs)),
	}
	for i := range runs {
		run := runs[i]
		if run.KeyCount() == 0 {
			continue
		}
		h.buf = append(h.buf, fieldStorageRunCursor{
			run: i,
			pos: 0,
			key: run.keyAt(0),
		})
	}
	if len(h.buf) == 0 {
		fieldStorageRunCursorSlicePool.Put(h.buf)
		ReleaseFieldStorageRunsOwned(runs)
		return FieldStorage{}
	}
	h.init()

	entries := GetFieldEntrySlice(min(total, fieldIndexChunkThreshold))
	var builder fieldStorageBuilder
	chunked := false

	for h.Len() > 0 {
		item := h.pop()
		run := &h.runs[item.run]
		key := item.key
		merged := run.takePosting(item.pos)
		h.push(item.run, item.pos+1)

		for h.Len() > 0 {
			next := h.buf[0]
			if keycodec.Compare(next.key, key) != 0 {
				break
			}
			item = h.pop()
			run = &h.runs[item.run]
			merged = merged.BuildMergedOwned(run.takePosting(item.pos))
			h.push(item.run, item.pos+1)
		}
		merged = merged.BuildOptimized()
		if !chunked {
			entries = append(entries, Entry{Key: key, IDs: merged})
			if len(entries) >= fieldIndexChunkThreshold {
				builder.init(total, numeric)
				for i := range entries {
					builder.append(entries[i].Key, entries[i].IDs)
				}
				ReleaseFieldEntrySlice(entries)
				entries = nil
				chunked = true
			}
			continue
		}
		builder.append(key, merged)
	}

	if !chunked {
		if len(entries) == 0 {
			ReleaseFieldEntrySlice(entries)
			fieldStorageRunCursorSlicePool.Put(h.buf)
			ReleaseFieldStorageRunsOwned(runs)
			return FieldStorage{}
		}
		storage := newFlatFieldStorage(entries, nil)
		fieldStorageRunCursorSlicePool.Put(h.buf)
		ReleaseFieldStorageRunsOwned(runs)
		return storage
	}

	storage := builder.finish()
	fieldStorageRunCursorSlicePool.Put(h.buf)
	ReleaseFieldStorageRunsOwned(runs)
	return storage
}

func NewNilFieldStorageOwned(ids posting.List) FieldStorage {
	ids = ids.BuildOptimized()
	if ids.IsEmpty() {
		return FieldStorage{}
	}
	entries := GetFieldEntrySlice(1)
	entries = append(entries, Entry{
		Key: keycodec.FromString(NilIndexEntryKey),
		IDs: ids,
	})
	return newFlatFieldStorage(entries, nil)
}

func NewFlatFieldStorageFromPostingMapOwned(m map[string]posting.List) FieldStorage {
	if len(m) == 0 {
		ReleasePostingMap(m)
		return FieldStorage{}
	}
	keys := pooled.GetStringSlice(len(m))
	for key, ids := range m {
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			continue
		}
		m[key] = ids
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		pooled.ReleaseStringSlice(keys)
		ReleasePostingMap(m)
		return FieldStorage{}
	}
	sort.Strings(keys)
	entries := GetFieldEntrySlice(len(keys))[:len(keys)]
	for i := range keys {
		entries[i] = Entry{
			Key: keycodec.FromString(keys[i]),
			IDs: m[keys[i]],
		}
	}
	pooled.ReleaseStringSlice(keys)
	ReleasePostingMap(m)
	return newFlatFieldStorage(entries, nil)
}

func NewRegularFieldStorageFromPostingMapOwned(m map[string]posting.List) FieldStorage {
	if len(m) == 0 {
		ReleasePostingMap(m)
		return FieldStorage{}
	}
	keys := pooled.GetStringSlice(len(m))
	for key, ids := range m {
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			continue
		}
		m[key] = ids
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		pooled.ReleaseStringSlice(keys)
		ReleasePostingMap(m)
		return FieldStorage{}
	}
	sort.Strings(keys)
	if len(keys) < fieldIndexChunkThreshold {
		entries := GetFieldEntrySlice(len(keys))[:len(keys)]
		for i := range keys {
			entries[i] = Entry{
				Key: keycodec.FromString(keys[i]),
				IDs: m[keys[i]],
			}
		}
		pooled.ReleaseStringSlice(keys)
		ReleasePostingMap(m)
		return newFlatFieldStorage(entries, nil)
	}

	builder := newFieldIndexChunkBuilder(len(keys))
	for start := 0; start < len(keys); {
		size := nextStringFieldIndexChunkSizeStrings(keys, start)
		end := start + size
		totalBytes := 0
		for _, key := range keys[start:end] {
			totalBytes += len(key)
		}
		data := fieldByteSlice(totalBytes)
		refs := fieldStringRefSlice(end - start)
		posts := fieldPostingSlice(end - start)
		off := 0
		var rows uint64
		for i, key := range keys[start:end] {
			n := copy(data[off:], key)
			refs[i] = newFieldIndexStringRef(off, n)
			posts[i] = m[key]
			rows += posts[i].Cardinality()
			off += n
		}
		builder.appendChunk(newStringFieldIndexChunk(posts, refs, data, rows))
		start = end
	}
	pooled.ReleaseStringSlice(keys)
	ReleasePostingMap(m)
	return newChunkedFieldStorage(builder.root())
}

func NewRegularFieldStorageFromFixedPostingMapOwned(m map[uint64]posting.List) FieldStorage {
	if len(m) == 0 {
		ReleaseFixedPostingMap(m)
		return FieldStorage{}
	}
	keys := pooled.GetUint64Slice(len(m))
	for key, ids := range m {
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			continue
		}
		m[key] = ids
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		pooled.ReleaseUint64Slice(keys)
		ReleaseFixedPostingMap(m)
		return FieldStorage{}
	}
	slices.Sort(keys)
	if len(keys) < fieldIndexChunkThreshold {
		entries := GetFieldEntrySlice(len(keys))[:len(keys)]
		for i := range keys {
			entries[i] = Entry{
				Key: keycodec.FromU64(keys[i]),
				IDs: m[keys[i]],
			}
		}
		pooled.ReleaseUint64Slice(keys)
		ReleaseFixedPostingMap(m)
		return newFlatFieldStorage(entries, nil)
	}

	builder := newFieldIndexChunkBuilder(len(keys))
	for start := 0; start < len(keys); {
		size := balancedFieldIndexChunkSize(len(keys) - start)
		end := start + size
		numeric := fieldUint64Slice(end - start)
		posts := fieldPostingSlice(end - start)
		copy(numeric, keys[start:end])
		var rows uint64
		for i, key := range keys[start:end] {
			posts[i] = m[key]
			rows += posts[i].Cardinality()
		}
		builder.appendChunk(newNumericFieldIndexChunk(posts, numeric, rows))
		start = end
	}
	pooled.ReleaseUint64Slice(keys)
	ReleaseFixedPostingMap(m)
	return newChunkedFieldStorage(builder.root())
}

func NewLenFieldStorageFromMapOwned(universe posting.List, lengths map[uint32]posting.List) (FieldStorage, bool) {
	return newLenFieldStorageFromMapOwned(universe, lengths, posting.List{})
}

func RebuildLenFieldStorageFromIndexView(universe posting.List, fieldOV FieldIndexView) (FieldStorage, bool) {
	if universe.IsEmpty() {
		return FieldStorage{}, false
	}

	var nonEmpty posting.List
	counts := lenCountMapPool.Get()

	br := fieldOV.RangeForBounds(Bounds{Has: true})
	if !br.Empty() {
		cur := fieldOV.NewCursor(br, false)
		for {
			_, ids, ok := cur.Next()
			if !ok {
				break
			}
			if ids.IsEmpty() {
				continue
			}
			if nonEmpty.IsEmpty() {
				nonEmpty = ids.Clone()
			} else {
				nonEmpty = nonEmpty.BuildOr(ids)
			}
			it := ids.Iter()
			for it.HasNext() {
				counts[it.Next()]++
			}
			it.Release()
		}
	}

	lengths := lenPostingMapPool.Get()
	for idx, ln := range counts {
		if ln == 0 {
			continue
		}
		ids := lengths[ln]
		ids = ids.BuildAdded(idx)
		lengths[ln] = ids
	}
	lenCountMapPool.Put(counts)

	empty := universe.Clone()
	empty = empty.BuildAndNot(nonEmpty)
	if !empty.IsEmpty() {
		lengths[0] = empty
	}

	lengthsLen := len(lengths)
	storage, useZeroComplement := newLenFieldStorageFromMapOwned(universe, lengths, nonEmpty)
	if lengthsLen <= LenPostingMapMaxRetainedLen {
		lenPostingMapPool.Put(lengths)
	}
	nonEmpty.Release()
	return storage, useZeroComplement
}

func newLenFieldStorageFromMapOwned(universe posting.List, lengths map[uint32]posting.List, nonEmpty posting.List) (FieldStorage, bool) {
	if len(lengths) == 0 || universe.IsEmpty() {
		posting.ReleaseMapU32(lengths)
		clear(lengths)
		return FieldStorage{}, false
	}

	empty := lengths[0]
	emptyCard := empty.Cardinality()
	nonEmptyCard := uint64(0)
	if nonEmpty.IsEmpty() {
		universeCard := universe.Cardinality()
		if universeCard > emptyCard {
			nonEmptyCard = universeCard - emptyCard
		}
	} else {
		nonEmptyCard = nonEmpty.Cardinality()
	}
	useZeroComplement := nonEmptyCard > 0 && nonEmptyCard < emptyCard

	keys := pooled.GetUint32Slice(len(lengths))
	for ln, ids := range lengths {
		if useZeroComplement && ln == 0 {
			continue
		}
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			delete(lengths, ln)
			continue
		}
		lengths[ln] = ids
		keys = append(keys, ln)
	}
	slices.Sort(keys)

	capHint := len(keys)
	if useZeroComplement {
		capHint++
	}
	entries := GetFieldEntrySlice(capHint)
	for i := range keys {
		ln := keys[i]
		entries = append(entries, Entry{
			Key: keycodec.FromU64(uint64(ln)),
			IDs: lengths[ln],
		})
		delete(lengths, ln)
	}
	pooled.ReleaseUint32Slice(keys)

	if useZeroComplement {
		var ids posting.List
		if nonEmpty.IsEmpty() {
			ids = universe.Clone()
			if !empty.IsEmpty() {
				ids = ids.BuildAndNot(empty)
			}
		} else {
			ids = nonEmpty.Clone()
		}
		empty.Release()
		delete(lengths, 0)

		ids = ids.BuildOptimized()
		if !ids.IsEmpty() {
			// The marker starts with 0xff, after every uint32 length encoded as u64.
			entries = append(entries, Entry{
				Key: keycodec.FromString(LenIndexNonEmptyKey),
				IDs: ids,
			})
		}
	}

	if len(entries) == 0 {
		ReleaseFieldEntrySlice(entries)
		return FieldStorage{}, false
	}
	return newFlatFieldStorage(entries, nil), useZeroComplement
}

func (s FieldStorage) IsChunked() bool {
	return s.chunked != nil
}

func (s FieldStorage) KeyCount() int {
	if s.chunked != nil {
		return s.chunked.keyCount
	}
	if s.flat == nil {
		return 0
	}
	return len(s.flat.entries)
}

func (chunk *fieldIndexChunk) keyCount() int {
	if chunk.stringRefs != nil {
		return len(chunk.stringRefs)
	}
	if chunk.posts == nil {
		return len(chunk.numeric) >> 1
	}
	return len(chunk.numeric)
}

func (chunk *fieldIndexChunk) keyAt(i int) keycodec.IndexKey {
	if chunk.stringRefs != nil {
		ref := chunk.stringRefs[i]
		return keycodec.FromBytes(chunk.stringData[fieldIndexStringRefOff(ref) : fieldIndexStringRefOff(ref)+fieldIndexStringRefLen(ref)])
	}
	if chunk.posts == nil {
		return keycodec.FromU64(chunk.numeric[i<<1])
	}
	return keycodec.FromU64(chunk.numeric[i])
}

func (chunk *fieldIndexChunk) postingAt(i int) posting.List {
	if chunk.stringRefs != nil && chunk.posts == nil {
		var p posting.List
		return p.BuildAdded(chunk.numeric[i])
	}
	if chunk.posts == nil {
		base := i << 1
		var p posting.List
		return p.BuildAdded(chunk.numeric[base+1])
	}
	return chunk.posts[i].Borrow()
}

func (chunk *fieldIndexChunk) rowCount() uint64 {
	return chunk.rows
}

func (chunk *fieldIndexChunk) rowsInRange(start, end int) uint64 {
	limit := len(chunk.posts)
	unique := chunk.posts == nil
	if chunk.stringRefs != nil {
		limit = len(chunk.stringRefs)
	} else if unique {
		limit = len(chunk.numeric) >> 1
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
	if unique {
		return uint64(end - start)
	}
	var total uint64
	for i := start; i < end; i++ {
		total += chunk.posts[i].Cardinality()
	}
	return total
}

func newFieldIndexChunkFromEntries(entries []Entry) *fieldIndexChunk {
	if len(entries) == 0 {
		return nil
	}
	posts := fieldPostingSlice(len(entries))
	var rows uint64
	if entries[0].Key.IsNumeric() {
		keys := fieldUint64Slice(len(entries))
		for i := range entries {
			keys[i] = entries[i].Key.U64()
			posts[i] = storedFieldPosting(entries[i].IDs)
			rows += posts[i].Cardinality()
		}
		return newNumericFieldIndexChunk(posts, keys, rows)
	}

	totalBytes := 0
	for i := range entries {
		totalBytes += entries[i].Key.ByteLen()
	}
	refs := fieldStringRefSlice(len(entries))
	data := fieldByteSlice(totalBytes)
	off := 0
	for i := range entries {
		s := entries[i].Key.UnsafeString()
		n := copy(data[off:], s)
		refs[i] = newFieldIndexStringRef(off, n)
		posts[i] = storedFieldPosting(entries[i].IDs)
		rows += posts[i].Cardinality()
		off += n
	}
	return newStringFieldIndexChunk(posts, refs, data, rows)
}

func newFieldIndexChunkRefsFromEntries(entries []Entry) []fieldIndexChunkRef {
	if len(entries) == 0 {
		return nil
	}

	refs := fieldIndexChunkRefSlicePool.Get(max(1, len(entries)/fieldIndexChunkTargetEntries+1))
	numeric := entries[0].Key.IsNumeric()

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

func (chunk *fieldIndexChunk) lowerBound(key string) int {
	lo, hi := 0, chunk.keyCount()
	for lo < hi {
		mid := (lo + hi) >> 1
		if keycodec.CompareString(chunk.keyAt(mid), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (chunk *fieldIndexChunk) lowerBoundKey(key keycodec.IndexKey) int {
	lo, hi := 0, chunk.keyCount()
	for lo < hi {
		mid := (lo + hi) >> 1
		if keycodec.Compare(chunk.keyAt(mid), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func lowerBoundIndexEntriesKey(entries []Entry, key keycodec.IndexKey) int {
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := (lo + hi) >> 1
		if keycodec.Compare(entries[mid].Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func upperBoundIndexEntriesKey(entries []Entry, key keycodec.IndexKey) int {
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := (lo + hi) >> 1
		if keycodec.Compare(entries[mid].Key, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (r *fieldIndexChunkedRoot) searchPageByLastKeyFrom(start int, key keycodec.IndexKey) int {
	lo, hi := start, len(r.pages)
	for lo < hi {
		mid := lo + (hi-lo)>>1
		if keycodec.Compare(r.pages[mid].lastKey(), key) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (p *fieldIndexChunkDirPage) searchRefByLastKeyFrom(start int, key keycodec.IndexKey) int {
	lo, hi := start, len(p.refs)
	for lo < hi {
		mid := lo + (hi-lo)>>1
		if keycodec.Compare(p.refs[mid].last, key) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (r *fieldIndexChunkedRoot) searchPageByLastString(key string) int {
	lo, hi := 0, len(r.pages)
	for lo < hi {
		mid := lo + (hi-lo)>>1
		if keycodec.CompareString(r.pages[mid].lastKey(), key) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (p *fieldIndexChunkDirPage) searchRefByLastString(key string) int {
	lo, hi := 0, len(p.refs)
	for lo < hi {
		mid := lo + (hi-lo)>>1
		if keycodec.CompareString(p.refs[mid].last, key) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (chunk *fieldIndexChunk) upperBound(key string) int {
	lo, hi := 0, chunk.keyCount()
	for lo < hi {
		mid := (lo + hi) >> 1
		if keycodec.CompareString(chunk.keyAt(mid), key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (chunk *fieldIndexChunk) upperBoundKey(key keycodec.IndexKey) int {
	lo, hi := 0, chunk.keyCount()
	for lo < hi {
		mid := (lo + hi) >> 1
		if keycodec.Compare(chunk.keyAt(mid), key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (chunk *fieldIndexChunk) lowerBoundPrefixUpperBound(upper keycodec.PrefixUpperBound) int {
	lo, hi := 0, chunk.keyCount()
	for lo < hi {
		mid := (lo + hi) >> 1
		if keycodec.ComparePrefixUpperBound(chunk.keyAt(mid), upper) < 0 {
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

func (ref fieldIndexChunkRef) retained() fieldIndexChunkRef {
	ref.chunk.retain()
	return ref
}

func newFieldIndexChunkStreamBuilder(numeric bool) fieldIndexChunkStreamBuilder {
	out := fieldIndexChunkStreamBuilder{
		numeric: numeric,
		posts:   posting.GetSlice(fieldIndexChunkTargetEntries),
	}
	if numeric {
		out.numericKey = pooled.GetUint64Slice(fieldIndexChunkTargetEntries)
	} else {
		out.stringKeys = keycodec.GetIndexKeySlice(fieldIndexChunkTargetEntries)
	}
	return out
}

func (b *fieldIndexChunkStreamBuilder) append(builder *fieldIndexChunkBuilder, key keycodec.IndexKey, ids posting.List) {
	if ids.IsEmpty() {
		return
	}
	if !b.numeric {
		keyLen := key.ByteLen()
		if keyLen > fieldIndexStringRefMax {
			panic(fmt.Sprintf("indexed string value len %d exceeds limit %d", keyLen, fieldIndexStringRefMax))
		}
		if len(b.posts) > 0 && shouldSealStringFieldIndexChunk(len(b.posts), b.stringBytes) {
			b.sealActiveChunk(builder)
		}
	}
	b.posts = append(b.posts, ids)
	b.rows += ids.Cardinality()
	if b.numeric {
		b.numericKey = append(b.numericKey, key.U64())
	} else {
		b.stringKeys = append(b.stringKeys, key)
		b.stringBytes += key.ByteLen()
	}
	if b.numeric {
		if len(b.posts) == fieldIndexChunkTargetEntries {
			b.sealActiveChunk(builder)
		}
		return
	}
	if shouldSealStringFieldIndexChunk(len(b.posts), b.stringBytes) {
		b.sealActiveChunk(builder)
	}
}

func (b *fieldIndexChunkStreamBuilder) publishPendingChunk(builder *fieldIndexChunkBuilder) {
	if len(b.pendingPosts) == 0 {
		return
	}
	if b.numeric {
		builder.appendChunk(newNumericFieldIndexChunk(b.pendingPosts, b.pendingNumericKey, b.pendingRows))
	} else {
		builder.appendChunk(newFieldIndexChunkFromKeys(b.pendingPosts, b.pendingStringKeys, b.pendingRows))
		b.freeStringKeys = b.pendingStringKeys[:0]
	}
	b.pendingPosts = nil
	b.pendingRows = 0
	b.pendingNumericKey = nil
	b.pendingStringKeys = nil
}

func (b *fieldIndexChunkStreamBuilder) releaseActiveScratch() {
	if b.posts != nil {
		posting.ReleaseAll(b.posts)
		posting.ReleaseSlice(b.posts)
		b.posts = nil
	}
	if b.numeric && b.numericKey != nil {
		pooled.ReleaseUint64Slice(b.numericKey)
		b.numericKey = nil
	}
	if !b.numeric && b.stringKeys != nil {
		keycodec.ReleaseIndexKeySlice(b.stringKeys)
	}
	if b.freeStringKeys != nil {
		keycodec.ReleaseIndexKeySlice(b.freeStringKeys)
	}
	b.stringKeys = nil
	b.freeStringKeys = nil
}

func (b *fieldIndexChunkStreamBuilder) releaseOwned() {
	if b.pendingPosts != nil {
		posting.ReleaseAll(b.pendingPosts)
		posting.ReleaseSlice(b.pendingPosts)
		b.pendingPosts = nil
	}
	if b.pendingNumericKey != nil {
		pooled.ReleaseUint64Slice(b.pendingNumericKey)
		b.pendingNumericKey = nil
	}
	if b.pendingStringKeys != nil {
		keycodec.ReleaseIndexKeySlice(b.pendingStringKeys)
		b.pendingStringKeys = nil
	}
	b.pendingRows = 0
	b.releaseActiveScratch()
}

func (b *fieldIndexChunkStreamBuilder) sealActiveChunk(builder *fieldIndexChunkBuilder) {
	if len(b.posts) == 0 {
		return
	}
	b.publishPendingChunk(builder)
	b.pendingPosts = b.posts
	b.pendingRows = b.rows
	if b.numeric {
		b.pendingNumericKey = b.numericKey
		b.numericKey = pooled.GetUint64Slice(fieldIndexChunkTargetEntries)
	} else {
		b.pendingStringKeys = b.stringKeys
		if cap(b.freeStringKeys) >= fieldIndexChunkTargetEntries {
			b.stringKeys = b.freeStringKeys[:0]
			b.freeStringKeys = nil
		} else {
			if b.freeStringKeys != nil {
				keycodec.ReleaseIndexKeySlice(b.freeStringKeys)
				b.freeStringKeys = nil
			}
			b.stringKeys = keycodec.GetIndexKeySlice(fieldIndexChunkTargetEntries)
		}
	}
	b.posts = posting.GetSlice(fieldIndexChunkTargetEntries)
	b.rows = 0
	b.stringBytes = 0
}

func (b *fieldIndexChunkStreamBuilder) finish(builder *fieldIndexChunkBuilder) {
	if len(b.pendingPosts) == 0 {
		if len(b.posts) == 0 {
			b.releaseActiveScratch()
			return
		}
		b.publishActiveChunk(builder)
		return
	}
	if len(b.posts) == 0 {
		b.publishPendingChunk(builder)
		b.releaseActiveScratch()
		return
	}
	if len(b.posts) >= fieldIndexChunkMinEntries {
		b.publishPendingChunk(builder)
		b.publishActiveChunk(builder)
		return
	}
	b.rebalancePendingWithTail(builder)
}

func newFieldIndexChunkFromKeys(posts []posting.List, keys []keycodec.IndexKey, rows uint64) *fieldIndexChunk {
	if len(posts) == 0 {
		return nil
	}
	for i := range posts {
		posts[i] = storedFieldPosting(posts[i])
	}
	totalBytes := 0
	for i := range keys {
		totalBytes += keys[i].ByteLen()
	}
	refs := fieldStringRefSlice(len(keys))
	data := fieldByteSlice(totalBytes)
	off := 0
	for i := range keys {
		n := copy(data[off:], keys[i].UnsafeString())
		refs[i] = newFieldIndexStringRef(off, n)
		off += n
	}
	return newStringFieldIndexChunk(posts, refs, data, rows)
}

func (chunk *fieldIndexChunk) numericRefsWithInsertedEntry(pos int, add Entry) []fieldIndexChunkRef {
	if chunk == nil {
		return nil
	}
	total := chunk.keyCount() + 1
	if total <= 0 {
		return nil
	}

	if total <= fieldIndexChunkMaxEntries {
		keys := fieldUint64Slice(total)
		posts := fieldPostingSlice(total)
		src := 0
		for dst := 0; dst < total; dst++ {
			if dst == pos {
				keys[dst] = add.Key.U64()
				posts[dst] = add.IDs
				continue
			}
			keys[dst] = chunk.keyAt(src).U64()
			posts[dst] = chunk.postingAt(src)
			src++
		}
		next := newNumericFieldIndexChunk(posts, keys, chunk.rows+add.IDs.Cardinality())
		refs := fieldIndexChunkRefSlicePool.Get(1)
		refs = append(refs, fieldIndexChunkRef{last: next.keyAt(total - 1), chunk: next})
		return refs
	}

	firstSize := balancedFieldIndexChunkSize(total)
	secondSize := total - firstSize

	firstKeys := fieldUint64Slice(firstSize)
	firstPosts := fieldPostingSlice(firstSize)
	secondKeys := fieldUint64Slice(secondSize)
	secondPosts := fieldPostingSlice(secondSize)

	var firstRows uint64
	var secondRows uint64

	src := 0
	for dst := 0; dst < total; dst++ {
		var key uint64
		var ids posting.List
		if dst == pos {
			key = add.Key.U64()
			ids = add.IDs
		} else {
			key = chunk.keyAt(src).U64()
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
	refs := fieldIndexChunkRefSlicePool.Get(2)
	refs = append(refs, fieldIndexChunkRef{last: first.keyAt(firstSize - 1), chunk: first})
	refs = append(refs, fieldIndexChunkRef{last: second.keyAt(secondSize - 1), chunk: second})
	return refs
}

func (chunk *fieldIndexChunk) stringRefsWithInsertedEntry(pos int, add Entry) []fieldIndexChunkRef {
	if chunk == nil {
		return nil
	}
	baseLen := chunk.keyCount()
	if baseLen == 0 {
		return nil
	}
	entries := GetFieldEntrySlice(baseLen + 1)[:baseLen+1]
	for i := 0; i < pos; i++ {
		entries[i] = Entry{Key: chunk.keyAt(i), IDs: chunk.postingAt(i)}
	}
	entries[pos] = Entry{Key: add.Key, IDs: add.IDs}
	for i := pos; i < baseLen; i++ {
		entries[i+1] = Entry{Key: chunk.keyAt(i), IDs: chunk.postingAt(i)}
	}
	if len(entries) <= fieldIndexChunkMaxEntries && stringEntriesFitSingleChunk(entries) {
		next := newFieldIndexChunkFromEntries(entries)
		ReleaseFieldEntrySlice(entries)
		refs := fieldIndexChunkRefSlicePool.Get(1)
		refs = append(refs, fieldIndexChunkRef{last: next.keyAt(len(entries) - 1), chunk: next})
		return refs
	}
	refs := newFieldIndexChunkRefsFromEntries(entries)
	ReleaseFieldEntrySlice(entries)
	return refs
}

func (chunk *fieldIndexChunk) refsWithInsertedEntry(pos int, add Entry) []fieldIndexChunkRef {
	if chunk == nil {
		return nil
	}
	if chunk.hasNumericKeys() {
		return chunk.numericRefsWithInsertedEntry(pos, add)
	}
	return chunk.stringRefsWithInsertedEntry(pos, add)
}

func (chunk *fieldIndexChunk) borrowEntries() []Entry {
	if chunk == nil || chunk.keyCount() == 0 {
		return nil
	}
	entries := GetFieldEntrySlice(chunk.keyCount())[:chunk.keyCount()]
	for i := range entries {
		entries[i] = Entry{
			Key: chunk.keyAt(i),
			IDs: chunk.postingAt(i),
		}
	}
	return entries
}

func (b *fieldIndexChunkStreamBuilder) publishActiveChunk(builder *fieldIndexChunkBuilder) {
	if len(b.posts) == 0 {
		return
	}
	posts := b.posts
	if b.numeric {
		builder.appendChunk(newNumericFieldIndexChunk(posts, b.numericKey, b.rows))
		b.numericKey = nil
	} else {
		builder.appendChunk(newFieldIndexChunkFromKeys(posts, b.stringKeys, b.rows))
		keycodec.ReleaseIndexKeySlice(b.stringKeys)
		b.stringKeys = nil
		if b.freeStringKeys != nil {
			keycodec.ReleaseIndexKeySlice(b.freeStringKeys)
			b.freeStringKeys = nil
		}
	}
	b.posts = nil
	b.rows = 0
	b.stringBytes = 0
}

func (b *fieldIndexChunkStreamBuilder) rebalancePendingWithTail(builder *fieldIndexChunkBuilder) {
	if len(b.pendingPosts) == 0 || len(b.posts) == 0 {
		return
	}
	total := len(b.pendingPosts) + len(b.posts)
	combinedPosts := posting.GetSlice(total)
	combinedPosts = append(combinedPosts, b.pendingPosts...)
	combinedPosts = append(combinedPosts, b.posts...)
	if b.numeric {
		firstSize := balancedFieldIndexChunkSize(total)
		secondSize := total - firstSize
		firstPosts := fieldPostingSlice(firstSize)
		secondPosts := fieldPostingSlice(secondSize)
		var firstRows uint64
		var secondRows uint64
		for i := 0; i < firstSize; i++ {
			ids := combinedPosts[i]
			firstPosts[i] = ids
			firstRows += ids.Cardinality()
		}
		for i := 0; i < secondSize; i++ {
			ids := combinedPosts[firstSize+i]
			secondPosts[i] = ids
			secondRows += ids.Cardinality()
		}
		combinedKeys := pooled.GetUint64Slice(total)
		combinedKeys = append(combinedKeys, b.pendingNumericKey...)
		combinedKeys = append(combinedKeys, b.numericKey...)
		firstKeys := fieldUint64Slice(firstSize)
		copy(firstKeys, combinedKeys[:firstSize])
		secondKeys := fieldUint64Slice(secondSize)
		copy(secondKeys, combinedKeys[firstSize:firstSize+secondSize])
		builder.appendChunk(newNumericFieldIndexChunk(firstPosts, firstKeys, firstRows))
		builder.appendChunk(newNumericFieldIndexChunk(secondPosts, secondKeys, secondRows))
		pooled.ReleaseUint64Slice(combinedKeys)
		pooled.ReleaseUint64Slice(b.pendingNumericKey)
		pooled.ReleaseUint64Slice(b.numericKey)
		b.pendingNumericKey = nil
		b.numericKey = nil
	} else {
		combinedKeys := keycodec.GetIndexKeySlice(total)
		combinedKeys = append(combinedKeys, b.pendingStringKeys...)
		combinedKeys = append(combinedKeys, b.stringKeys...)
		for start := 0; start < len(combinedKeys); {
			size := nextStringFieldIndexChunkSizeIndexKeys(combinedKeys, start)
			end := start + size
			chunkPosts := fieldPostingSlice(size)
			var rows uint64
			for i := 0; i < size; i++ {
				ids := combinedPosts[start+i]
				chunkPosts[i] = ids
				rows += ids.Cardinality()
			}
			builder.appendChunk(newFieldIndexChunkFromKeys(chunkPosts, combinedKeys[start:end], rows))
			start = end
		}
		keycodec.ReleaseIndexKeySlice(combinedKeys)
		keycodec.ReleaseIndexKeySlice(b.pendingStringKeys)
		keycodec.ReleaseIndexKeySlice(b.stringKeys)
		b.pendingStringKeys = nil
		b.stringKeys = nil
	}
	posting.ReleaseSlice(b.pendingPosts)
	posting.ReleaseSlice(b.posts)
	b.pendingPosts = nil
	b.pendingRows = 0
	posting.ReleaseSlice(combinedPosts)
	b.posts = nil
	b.rows = 0
	b.stringBytes = 0
}

func newFieldIndexChunkBuilder(capEntries int) fieldIndexChunkBuilder {
	chunkCap := max(1, capEntries/fieldIndexChunkTargetEntries+1)
	pageCap := max(1, chunkCap/fieldIndexDirPageTargetRefs+2)
	return fieldIndexChunkBuilder{
		pages:       fieldIndexChunkDirPageSlicePool.Get(pageCap),
		pendingRefs: fieldIndexChunkRefSlicePool.Get(min(chunkCap, fieldIndexDirPageTargetRefs)),
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
	startPageRef := root.pages[startPage]
	if startPage == endPage {
		b.appendRefSlice(startPageRef, startOff, endOff)
		return
	}
	if startOff > 0 {
		b.appendRefSlice(startPageRef, startOff, len(startPageRef.refs))
		startPage++
	}
	for page := startPage; page < endPage; page++ {
		b.appendPage(root.pages[page])
	}
	if endOff > 0 {
		b.appendRefSlice(root.pages[endPage], 0, endOff)
	}
}

func (b *fieldIndexChunkBuilder) appendEntries(entries []Entry) {
	if b == nil || len(entries) == 0 {
		return
	}
	numeric := entries[0].Key.IsNumeric()
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
	if page == nil || start < 0 || end < start || end > len(page.refs) {
		return
	}
	for i := start; i < end; i++ {
		b.appendOwnedRef(page.refs[i].retained())
	}
}

func (b *fieldIndexChunkBuilder) flushPendingPage() {
	if b == nil || b.pendingRefs == nil || len(b.pendingRefs) == 0 {
		return
	}
	page := newFieldIndexChunkDirPage(b.pendingRefs)
	b.pages = append(b.pages, page)
	b.pendingRefs = b.pendingRefs[:0]
}

func (b *fieldIndexChunkBuilder) releaseOwned() {
	if b.pendingRefs != nil {
		releaseOwnedFieldIndexChunkRefSlice(b.pendingRefs)
		b.pendingRefs = nil
	}
	releaseFieldIndexChunkDirPages(b.pages)
	b.pages = nil
	b.total = 0
	b.chunks = 0
}

func (b *fieldIndexChunkBuilder) appendPage(page *fieldIndexChunkDirPage) {
	if b == nil || page == nil || len(page.refs) == 0 {
		return
	}
	if len(page.refs) != fieldIndexDirPageTargetRefs || len(b.pendingRefs) != 0 {
		b.appendRefSlice(page, 0, len(page.refs))
		return
	}
	page.retain()
	b.pages = append(b.pages, page)
	b.total += page.keyCount()
	b.chunks += len(page.refs)
}

func (b *fieldIndexChunkBuilder) appendOwnedPage(page *fieldIndexChunkDirPage) {
	if b == nil || page == nil || len(page.refs) == 0 {
		return
	}
	if len(b.pendingRefs) != 0 {
		b.flushPendingPage()
	}
	b.pages = append(b.pages, page)
	b.total += page.keyCount()
	b.chunks += len(page.refs)
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

func newFieldIndexChunkedRootFromOwnedPages(pages []*fieldIndexChunkDirPage) *fieldIndexChunkedRoot {
	if len(pages) == 0 {
		return nil
	}
	chunkPrefix := pooled.GetIntSlice(len(pages) + 1)[:len(pages)+1]
	chunkPrefix[0] = 0
	prefix := pooled.GetIntSlice(len(pages) + 1)[:len(pages)+1]
	prefix[0] = 0
	rowPrefix := pooled.GetUint64Slice(len(pages) + 1)[:len(pages)+1]
	rowPrefix[0] = 0

	chunks := 0
	total := 0
	rows := uint64(0)

	for i := 0; i < len(pages); i++ {
		page := pages[i]
		chunks += len(page.refs)
		total += page.keyCount()
		rows += page.rowPrefix[len(page.refs)]
		chunkPrefix[i+1] = chunks
		prefix[i+1] = total
		rowPrefix[i+1] = rows
	}
	if total == 0 {
		releaseFieldIndexChunkDirPages(pages)
		pooled.ReleaseIntSlice(chunkPrefix)
		pooled.ReleaseIntSlice(prefix)
		pooled.ReleaseUint64Slice(rowPrefix)
		return nil
	}
	root := fieldIndexChunkedRootPool.Get()
	root.pages = pages
	root.chunkPrefix = chunkPrefix
	root.prefix = prefix
	root.rowPrefix = rowPrefix
	root.keyCount = total
	root.chunkCount = chunks
	root.refs.Store(1)
	return root
}

func buildChunkedFieldIndexRoot(entries []Entry) *fieldIndexChunkedRoot {
	if len(entries) == 0 {
		return nil
	}
	builder := newFieldIndexChunkBuilder(len(entries))
	builder.appendEntries(entries)
	return builder.root()
}

func (r *fieldIndexChunkedRoot) flatten() ([]Entry, []byte) {
	if r == nil || r.keyCount == 0 {
		return nil, nil
	}
	out := GetFieldEntrySlice(r.keyCount)[:r.keyCount]
	var data []byte
	stringBytes := 0
	for i := 0; i < len(r.pages); i++ {
		page := r.pages[i]
		for j := 0; j < len(page.refs); j++ {
			chunk := page.refs[j].chunk
			if chunk.stringRefs != nil {
				stringBytes += len(chunk.stringData)
			}
		}
	}
	if stringBytes > 0 {
		data = pooled.GetByteSlice(stringBytes)
	}
	pos := 0
	for i := 0; i < len(r.pages); i++ {
		page := r.pages[i]
		for j := 0; j < len(page.refs); j++ {
			chunk := page.refs[j].chunk
			for k := 0; k < chunk.keyCount(); k++ {
				key := chunk.keyAt(k)
				if !key.IsNumeric() {
					s := key.UnsafeString()
					if len(s) > 0 {
						start := len(data)
						data = append(data, s...)
						key = keycodec.FromBytes(data[start:])
					}
				}
				out[pos] = Entry{
					Key: key,
					IDs: chunk.postingAt(k),
				}
				pos++
			}
		}
	}
	if pos == 0 {
		ReleaseFieldEntrySlice(out)
		pooled.ReleaseByteSlice(data)
		return nil, nil
	}
	out = out[:pos]
	return out, data
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

func (r *fieldIndexChunkedRoot) posKey(pos fieldIndexChunkPos) (keycodec.IndexKey, bool) {
	ref, ok := r.refAtChunk(pos.chunk)
	if !ok {
		return keycodec.IndexKey{}, false
	}
	if pos.entry < 0 || pos.entry >= ref.chunk.keyCount() {
		return keycodec.IndexKey{}, false
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
	if r == nil || r.pages == nil || len(r.pages) == 0 {
		return 0, 0
	}
	if chunk < 0 || chunk >= r.chunkCount {
		return len(r.pages), 0
	}
	page := searchIntUpper(r.chunkPrefix[1:], chunk)
	if page >= len(r.pages) {
		return len(r.pages), 0
	}
	return page, chunk - r.chunkPrefix[page]
}

func (r *fieldIndexChunkedRoot) refAtChunk(chunk int) (fieldIndexChunkRef, bool) {
	page, off := r.pagePosForChunk(chunk)
	if r.pages == nil || page >= len(r.pages) {
		return fieldIndexChunkRef{}, false
	}
	return r.pages[page].refs[off], true
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
	page := searchIntLower(r.chunkPrefix[1:], limit)
	if r.pages == nil || page >= len(r.pages) {
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
	page := searchIntUpper(r.prefix[1:], rank)
	if page >= len(r.pages) {
		return r.endPos()
	}
	pageRef := r.pages[page]
	localRank := rank - r.prefix[page]
	refIdx := searchIntUpper(pageRef.prefix[1:], localRank)
	if refIdx >= len(pageRef.refs) {
		return r.endPos()
	}
	return fieldIndexChunkPos{
		chunk: r.chunkPrefix[page] + refIdx,
		entry: localRank - pageRef.prefix[refIdx],
	}
}

func (r *fieldIndexChunkedRoot) lowerBoundPos(key string) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, 0
	}
	page := r.searchPageByLastString(key)
	if page >= len(r.pages) {
		return r.endPos(), r.keyCount
	}
	pageRef := r.pages[page]
	refIdx := pageRef.searchRefByLastString(key)
	if refIdx >= len(pageRef.refs) {
		return r.endPos(), r.keyCount
	}
	ref := pageRef.refs[refIdx]
	chunk := ref.chunk
	entryIdx := chunk.lowerBound(key)
	rank := r.prefix[page] + pageRef.prefix[refIdx] + entryIdx
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
	pageLo, pageHi := 0, len(r.pages)
	for pageLo < pageHi {
		mid := pageLo + (pageHi-pageLo)>>1
		if keycodec.CompareString(r.pages[mid].lastKey(), key) > 0 {
			pageHi = mid
		} else {
			pageLo = mid + 1
		}
	}
	page := pageLo
	if page >= len(r.pages) {
		return r.endPos(), r.keyCount
	}
	pageRef := r.pages[page]
	refLo, refHi := 0, len(pageRef.refs)
	for refLo < refHi {
		mid := refLo + (refHi-refLo)>>1
		if keycodec.CompareString(pageRef.refs[mid].last, key) > 0 {
			refHi = mid
		} else {
			refLo = mid + 1
		}
	}
	refIdx := refLo
	if refIdx >= len(pageRef.refs) {
		return r.endPos(), r.keyCount
	}
	ref := pageRef.refs[refIdx]
	chunk := ref.chunk
	entryIdx := chunk.upperBound(key)
	rank := r.prefix[page] + pageRef.prefix[refIdx] + entryIdx
	if entryIdx >= chunk.keyCount() {
		nextChunk := r.chunkPrefix[page] + refIdx + 1
		if nextChunk >= r.chunkCount {
			return r.endPos(), r.keyCount
		}
		return fieldIndexChunkPos{chunk: nextChunk}, rank
	}
	return fieldIndexChunkPos{chunk: r.chunkPrefix[page] + refIdx, entry: entryIdx}, rank
}

func (r *fieldIndexChunkedRoot) lowerBoundPosKey(key keycodec.IndexKey) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, 0
	}
	page := r.searchPageByLastKeyFrom(0, key)
	if page >= len(r.pages) {
		return r.endPos(), r.keyCount
	}
	pageRef := r.pages[page]
	refIdx := pageRef.searchRefByLastKeyFrom(0, key)
	if refIdx >= len(pageRef.refs) {
		return r.endPos(), r.keyCount
	}
	ref := pageRef.refs[refIdx]
	chunk := ref.chunk
	entryIdx := chunk.lowerBoundKey(key)
	rank := r.prefix[page] + pageRef.prefix[refIdx] + entryIdx
	if entryIdx >= chunk.keyCount() {
		nextChunk := r.chunkPrefix[page] + refIdx + 1
		if nextChunk >= r.chunkCount {
			return r.endPos(), r.keyCount
		}
		return fieldIndexChunkPos{chunk: nextChunk}, rank
	}
	return fieldIndexChunkPos{chunk: r.chunkPrefix[page] + refIdx, entry: entryIdx}, rank
}

func (r *fieldIndexChunkedRoot) upperBoundPosKey(key keycodec.IndexKey) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, 0
	}
	pageLo, pageHi := 0, len(r.pages)
	for pageLo < pageHi {
		mid := pageLo + (pageHi-pageLo)>>1
		if keycodec.Compare(r.pages[mid].lastKey(), key) > 0 {
			pageHi = mid
		} else {
			pageLo = mid + 1
		}
	}
	page := pageLo
	if page >= len(r.pages) {
		return r.endPos(), r.keyCount
	}
	pageRef := r.pages[page]
	refLo, refHi := 0, len(pageRef.refs)
	for refLo < refHi {
		mid := refLo + (refHi-refLo)>>1
		if keycodec.Compare(pageRef.refs[mid].last, key) > 0 {
			refHi = mid
		} else {
			refLo = mid + 1
		}
	}
	refIdx := refLo
	if refIdx >= len(pageRef.refs) {
		return r.endPos(), r.keyCount
	}
	ref := pageRef.refs[refIdx]
	chunk := ref.chunk
	entryIdx := chunk.upperBoundKey(key)
	rank := r.prefix[page] + pageRef.prefix[refIdx] + entryIdx
	if entryIdx >= chunk.keyCount() {
		nextChunk := r.chunkPrefix[page] + refIdx + 1
		if nextChunk >= r.chunkCount {
			return r.endPos(), r.keyCount
		}
		return fieldIndexChunkPos{chunk: nextChunk}, rank
	}
	return fieldIndexChunkPos{chunk: r.chunkPrefix[page] + refIdx, entry: entryIdx}, rank
}

func (r *fieldIndexChunkedRoot) lowerBoundPosPrefixUpperBound(upper keycodec.PrefixUpperBound) (fieldIndexChunkPos, int) {
	if r == nil || r.chunkCount == 0 {
		return fieldIndexChunkPos{}, 0
	}
	pageLo, pageHi := 0, len(r.pages)
	for pageLo < pageHi {
		mid := pageLo + (pageHi-pageLo)>>1
		if keycodec.ComparePrefixUpperBound(r.pages[mid].lastKey(), upper) >= 0 {
			pageHi = mid
		} else {
			pageLo = mid + 1
		}
	}
	page := pageLo
	if page >= len(r.pages) {
		return r.endPos(), r.keyCount
	}
	pageRef := r.pages[page]
	refLo, refHi := 0, len(pageRef.refs)
	for refLo < refHi {
		mid := refLo + (refHi-refLo)>>1
		if keycodec.ComparePrefixUpperBound(pageRef.refs[mid].last, upper) >= 0 {
			refHi = mid
		} else {
			refLo = mid + 1
		}
	}
	refIdx := refLo
	if refIdx >= len(pageRef.refs) {
		return r.endPos(), r.keyCount
	}
	ref := pageRef.refs[refIdx]
	chunk := ref.chunk
	entryIdx := chunk.lowerBoundPrefixUpperBound(upper)
	rank := r.prefix[page] + pageRef.prefix[refIdx] + entryIdx
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
	if key, ok := r.posKey(start); !ok || keycodec.CompareString(key, prefix) < 0 || !keycodec.HasPrefixString(key, prefix) {
		return start, startRank
	}
	upper, ok := keycodec.NewPrefixUpperBound(prefix)
	if !ok {
		return r.endPos(), r.keyCount
	}
	return r.lowerBoundPosPrefixUpperBound(upper)
}

func (r *fieldIndexChunkedRoot) lookupPostingRetained(key string) posting.List {
	if r == nil || r.chunkCount == 0 {
		return posting.List{}
	}
	page := r.searchPageByLastString(key)
	if page >= len(r.pages) {
		return posting.List{}
	}
	pageRef := r.pages[page]
	refIdx := pageRef.searchRefByLastString(key)
	if refIdx >= len(pageRef.refs) {
		return posting.List{}
	}
	ref := pageRef.refs[refIdx]
	chunk := ref.chunk
	entryIdx := chunk.lowerBound(key)
	if entryIdx >= chunk.keyCount() || !keycodec.EqualsString(chunk.keyAt(entryIdx), key) {
		return posting.List{}
	}
	return chunk.postingAt(entryIdx)
}

func (r *fieldIndexChunkedRoot) lookupPostingRetainedKey(key keycodec.IndexKey) posting.List {
	if r == nil || r.chunkCount == 0 {
		return posting.List{}
	}
	pos, _ := r.lowerBoundPosKey(key)
	ref, ok := r.refAtChunk(pos.chunk)
	if !ok || pos.entry >= ref.chunk.keyCount() || keycodec.Compare(ref.chunk.keyAt(pos.entry), key) != 0 {
		return posting.List{}
	}
	return ref.chunk.postingAt(pos.entry)
}

func (r *fieldIndexChunkedRoot) lookupCardinality(key string) uint64 {
	if r == nil || r.chunkCount == 0 {
		return 0
	}
	page := r.searchPageByLastString(key)
	if page >= len(r.pages) {
		return 0
	}
	pageRef := r.pages[page]
	refIdx := pageRef.searchRefByLastString(key)
	if refIdx >= len(pageRef.refs) {
		return 0
	}
	ref := pageRef.refs[refIdx]
	chunk := ref.chunk
	entryIdx := chunk.lowerBound(key)
	if entryIdx >= chunk.keyCount() || !keycodec.EqualsString(chunk.keyAt(entryIdx), key) {
		return 0
	}
	if chunk.posts == nil {
		return 1
	}
	return chunk.posts[entryIdx].Cardinality()
}

func (r *fieldIndexChunkedRoot) lookupCardinalityKey(key keycodec.IndexKey) uint64 {
	if r == nil || r.chunkCount == 0 {
		return 0
	}
	pos, _ := r.lowerBoundPosKey(key)
	ref, ok := r.refAtChunk(pos.chunk)
	if !ok || pos.entry >= ref.chunk.keyCount() || keycodec.Compare(ref.chunk.keyAt(pos.entry), key) != 0 {
		return 0
	}
	if ref.chunk.posts == nil {
		return 1
	}
	return ref.chunk.posts[pos.entry].Cardinality()
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

func (r *fieldIndexChunkedRoot) touchChunkIndexFrom(start int, key keycodec.IndexKey) int {
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
	page := r.searchPageByLastKeyFrom(startPage, key)
	if r.pages == nil || page >= len(r.pages) {
		return r.chunkCount - 1
	}
	pageRef := r.pages[page]
	refStart := 0
	if page == startPage {
		refStart = startOff
	}
	refIdx := pageRef.searchRefByLastKeyFrom(refStart, key)
	if refIdx >= len(pageRef.refs) {
		return r.chunkPrefix[page+1] - 1
	}
	return r.chunkPrefix[page] + refIdx
}

func searchIntLower(s []int, v int) int {
	l := len(s)
	i, j := 0, l
	for i < j {
		h := int(uint(i+j) >> 1)
		if s[h] < v {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func searchIntUpper(s []int, v int) int {
	l := len(s)
	i, j := 0, l
	for i < j {
		h := int(uint(i+j) >> 1)
		if s[h] <= v {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}
