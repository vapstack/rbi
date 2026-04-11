package rbi

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"go.etcd.io/bbolt"
)

var indexKeyNumericSentinel = new(byte)

// indexKey stores either:
// - numeric fixed-width key as uint64 (ptr == indexKeyNumericSentinel), or
// - string key bytes (ptr points to first byte, meta stores length).
//
// It is intentionally compact to keep index entry footprint low.
type indexKey struct {
	ptr  *byte
	meta uint64
}

func releaseBuildIndexRuns(runs []buildIndexFieldRun) {
	for i := range runs {
		runs[i].release()
	}
}

func cleanupBuildIndexFailure(buildOK *bool, fieldStates []*buildIndexFieldState, localUniverse []posting.List) {
	if *buildOK {
		return
	}
	for i := range fieldStates {
		fieldStates[i].release()
	}
	for i := range localUniverse {
		localUniverse[i].Release()
	}
}

func releaseBuildIndexLocalStates(localStates []buildIndexFieldLocalState) {
	for i := range localStates {
		localStates[i].release()
	}
}

func recoverLoadIndex(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("loadIndex panic: %v", r)
	}
}

func indexKeyFromString(s string) indexKey {
	if len(s) == 0 {
		return indexKey{}
	}
	return indexKey{
		ptr:  unsafe.StringData(s),
		meta: uint64(len(s)),
	}
}

func indexKeyFromBytes(b []byte) indexKey {
	if len(b) == 0 {
		return indexKey{}
	}
	return indexKey{
		ptr:  unsafe.SliceData(b),
		meta: uint64(len(b)),
	}
}

func indexKeyFromU64(v uint64) indexKey {
	return indexKey{
		ptr:  indexKeyNumericSentinel,
		meta: v,
	}
}

func indexKeyFromFixed8String(s string) indexKey {
	if len(s) != 8 {
		return indexKeyFromString(s)
	}
	return indexKeyFromU64(fixed8StringToU64(s))
}

func (k indexKey) isNumeric() bool {
	return k.ptr == indexKeyNumericSentinel
}

func (k indexKey) byteLen() int {
	if k.isNumeric() {
		return 8
	}
	if k.ptr == nil {
		return 0
	}
	if k.meta > uint64(^uint(0)>>1) {
		panic("index key len overflows int")
	}
	return int(k.meta)
}

func (k indexKey) asUnsafeString() string {
	if k.isNumeric() {
		return uint64ByteStr(k.meta)
	}
	if k.ptr == nil {
		return ""
	}
	return unsafe.String(k.ptr, k.byteLen())
}

func (k indexKey) byteAt(i int) byte {
	if k.isNumeric() {
		shift := uint(56 - i*8)
		return byte(k.meta >> shift)
	}
	return *(*byte)(unsafe.Add(unsafe.Pointer(k.ptr), i))
}

func fixed8StringToU64(s string) uint64 {
	_ = s[7]
	return uint64(s[0])<<56 |
		uint64(s[1])<<48 |
		uint64(s[2])<<40 |
		uint64(s[3])<<32 |
		uint64(s[4])<<24 |
		uint64(s[5])<<16 |
		uint64(s[6])<<8 |
		uint64(s[7])
}

func compareIndexKeys(a, b indexKey) int {
	if a.isNumeric() && b.isNumeric() {
		if a.meta < b.meta {
			return -1
		}
		if a.meta > b.meta {
			return 1
		}
		return 0
	}
	if !a.isNumeric() && !b.isNumeric() {
		return strings.Compare(a.asUnsafeString(), b.asUnsafeString())
	}
	alen := a.byteLen()
	blen := b.byteLen()
	n := min(alen, blen)
	for i := 0; i < n; i++ {
		ab := a.byteAt(i)
		bb := b.byteAt(i)
		if ab < bb {
			return -1
		}
		if ab > bb {
			return 1
		}
	}
	if alen < blen {
		return -1
	}
	if alen > blen {
		return 1
	}
	return 0
}

func compareIndexKeyString(a indexKey, b string) int {
	if a.isNumeric() && len(b) == 8 {
		v := fixed8StringToU64(b)
		if a.meta < v {
			return -1
		}
		if a.meta > v {
			return 1
		}
		return 0
	}
	if !a.isNumeric() {
		return strings.Compare(a.asUnsafeString(), b)
	}
	alen := a.byteLen()
	blen := len(b)
	n := min(alen, blen)
	for i := 0; i < n; i++ {
		ab := a.byteAt(i)
		bb := b[i]
		if ab < bb {
			return -1
		}
		if ab > bb {
			return 1
		}
	}
	if alen < blen {
		return -1
	}
	if alen > blen {
		return 1
	}
	return 0
}

func indexKeyEqualsString(a indexKey, b string) bool {
	if a.isNumeric() {
		return len(b) == 8 && a.meta == fixed8StringToU64(b)
	}
	return a.asUnsafeString() == b
}

func indexKeyHasPrefixString(a indexKey, prefix string) bool {
	if len(prefix) == 0 {
		return true
	}
	alen := a.byteLen()
	if len(prefix) > alen {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		if a.byteAt(i) != prefix[i] {
			return false
		}
	}
	return true
}

type prefixUpperBound struct {
	prefix   string
	cutLen   int
	lastByte byte
}

func newPrefixUpperBound(prefix string) (prefixUpperBound, bool) {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] == 0xFF {
			continue
		}
		return prefixUpperBound{
			prefix:   prefix,
			cutLen:   i + 1,
			lastByte: prefix[i] + 1,
		}, true
	}
	return prefixUpperBound{}, false
}

func compareStringPrefixUpperBound(s string, upper prefixUpperBound) int {
	n := min(len(s), upper.cutLen)
	last := upper.cutLen - 1
	for i := 0; i < n; i++ {
		ub := upper.prefix[i]
		if i == last {
			ub = upper.lastByte
		}
		if s[i] < ub {
			return -1
		}
		if s[i] > ub {
			return 1
		}
	}
	if len(s) < upper.cutLen {
		return -1
	}
	if len(s) > upper.cutLen {
		return 1
	}
	return 0
}

func compareIndexKeyPrefixUpperBound(a indexKey, upper prefixUpperBound) int {
	alen := a.byteLen()
	n := min(alen, upper.cutLen)
	last := upper.cutLen - 1
	for i := 0; i < n; i++ {
		ub := upper.prefix[i]
		if i == last {
			ub = upper.lastByte
		}
		ab := a.byteAt(i)
		if ab < ub {
			return -1
		}
		if ab > ub {
			return 1
		}
	}
	if alen < upper.cutLen {
		return -1
	}
	if alen > upper.cutLen {
		return 1
	}
	return 0
}

func indexKeyHasSuffixString(a indexKey, suffix string) bool {
	if len(suffix) == 0 {
		return true
	}
	alen := a.byteLen()
	if len(suffix) > alen {
		return false
	}
	start := alen - len(suffix)
	for i := 0; i < len(suffix); i++ {
		if a.byteAt(start+i) != suffix[i] {
			return false
		}
	}
	return true
}

func indexKeyContainsString(a indexKey, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	alen := a.byteLen()
	nlen := len(needle)
	if nlen > alen {
		return false
	}
	limit := alen - nlen
	for i := 0; i <= limit; i++ {
		if a.byteAt(i) != needle[0] {
			continue
		}
		ok := true
		for j := 1; j < nlen; j++ {
			if a.byteAt(i+j) != needle[j] {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

/**/

type buildIndexFieldState struct {
	numeric bool
	runsMu  sync.Mutex
	runs    []buildIndexFieldRun

	nilMu sync.Mutex
	nils  posting.List

	lenMu  sync.Mutex
	lenMap map[uint32]posting.List
}

type buildIndexFieldLocalState struct {
	numeric bool
	vals    map[string]posting.List
	fixed   map[uint64]posting.List
	lenMap  map[uint32]posting.List
	nils    posting.List
}

type buildIndexRunCursor struct {
	run int
	pos int
	key indexKey
}

type buildIndexRunHeap struct {
	runs []buildIndexFieldRun
	buf  []buildIndexRunCursor
}

type buildIndexFieldRun struct {
	stringBuf *pooled.SliceBuf[string]
	u64Buf    *pooled.SliceBuf[uint64]
	postBuf   *pooled.SliceBuf[posting.List]
}

func buildIndexRunTargetEntries() int {
	return max(postingMapPoolMaxLen, fieldIndexChunkTargetEntries*16)
}

func newBuildIndexFieldState(numeric bool, slice bool) *buildIndexFieldState {
	state := &buildIndexFieldState{numeric: numeric}
	if slice {
		state.lenMap = make(map[uint32]posting.List, 8)
	}
	return state
}

func newBuildIndexFieldLocalState(numeric bool, slice bool) buildIndexFieldLocalState {
	state := buildIndexFieldLocalState{numeric: numeric}
	if slice {
		state.lenMap = make(map[uint32]posting.List, 8)
	}
	return state
}

func (s *buildIndexFieldLocalState) addValue(key string, idx uint64) {
	if s.vals == nil {
		s.vals = postingMapPool.Get()
	}
	s.vals[key] = s.vals[key].BuildAdded(idx)
}

func (s *buildIndexFieldLocalState) addFixedValue(key uint64, idx uint64) {
	if s.fixed == nil {
		s.fixed = make(map[uint64]posting.List, 8)
	}
	s.fixed[key] = s.fixed[key].BuildAdded(idx)
}

func (s *buildIndexFieldLocalState) addNil(idx uint64) {
	s.nils = s.nils.BuildAdded(idx)
}

func (s *buildIndexFieldLocalState) addLen(length int, idx uint64) {
	if length < 0 {
		return
	}
	if s.lenMap == nil {
		s.lenMap = make(map[uint32]posting.List, 8)
	}
	ln := uint32(length)
	s.lenMap[ln] = s.lenMap[ln].BuildAdded(idx)
}

func (s *buildIndexFieldLocalState) shouldFlushRegular() bool {
	if s.numeric {
		return len(s.fixed) >= buildIndexRunTargetEntries()
	}
	return len(s.vals) >= buildIndexRunTargetEntries()
}

func (r buildIndexFieldRun) keyCount() int {
	if r.u64Buf != nil {
		return r.u64Buf.Len()
	}
	if r.stringBuf == nil {
		return 0
	}
	return r.stringBuf.Len()
}

func (r buildIndexFieldRun) keyAt(i int) indexKey {
	if r.u64Buf != nil {
		return indexKeyFromU64(r.u64Buf.Get(i))
	}
	return indexKeyFromString(r.stringBuf.Get(i))
}

func (r *buildIndexFieldRun) takePosting(i int) posting.List {
	ids := r.postBuf.Get(i)
	r.postBuf.Set(i, posting.List{})
	return ids
}

func (r *buildIndexFieldRun) release() {
	if r == nil {
		return
	}
	if r.stringBuf != nil {
		stringSlicePool.Put(r.stringBuf)
		r.stringBuf = nil
	}
	if r.u64Buf != nil {
		uint64SlicePool.Put(r.u64Buf)
		r.u64Buf = nil
	}
	if r.postBuf != nil {
		for i := 0; i < r.postBuf.Len(); i++ {
			ids := r.postBuf.Get(i)
			ids.Release()
			r.postBuf.Set(i, posting.List{})
		}
		postingSlicePool.Put(r.postBuf)
		r.postBuf = nil
	}
}

func buildIndexStringRunFromPostingMap(m map[string]posting.List) buildIndexFieldRun {
	if len(m) == 0 {
		return buildIndexFieldRun{}
	}
	keyBuf := stringSlicePool.Get()
	keyBuf.Grow(len(m))
	for key, ids := range m {
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			delete(m, key)
			continue
		}
		m[key] = ids
		keyBuf.Append(key)
	}
	if keyBuf.Len() == 0 {
		stringSlicePool.Put(keyBuf)
		return buildIndexFieldRun{}
	}
	pooled.SortSlice(keyBuf)
	postBuf := postingSlicePool.Get()
	postBuf.SetLen(keyBuf.Len())
	for i := 0; i < keyBuf.Len(); i++ {
		postBuf.Set(i, m[keyBuf.Get(i)])
	}
	clear(m)
	return buildIndexFieldRun{
		stringBuf: keyBuf,
		postBuf:   postBuf,
	}
}

func buildIndexFixedRunFromPostingMap(m map[uint64]posting.List) buildIndexFieldRun {
	if len(m) == 0 {
		return buildIndexFieldRun{}
	}
	keyBuf := uint64SlicePool.Get()
	keyBuf.Grow(len(m))
	for key, ids := range m {
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			delete(m, key)
			continue
		}
		m[key] = ids
		keyBuf.Append(key)
	}
	if keyBuf.Len() == 0 {
		uint64SlicePool.Put(keyBuf)
		return buildIndexFieldRun{}
	}
	pooled.SortSlice(keyBuf)
	postBuf := postingSlicePool.Get()
	postBuf.SetLen(keyBuf.Len())
	for i := 0; i < keyBuf.Len(); i++ {
		postBuf.Set(i, m[keyBuf.Get(i)])
	}
	clear(m)
	return buildIndexFieldRun{
		u64Buf:  keyBuf,
		postBuf: postBuf,
	}
}

func (s *buildIndexFieldState) appendRun(run buildIndexFieldRun) {
	if s == nil || run.keyCount() == 0 {
		return
	}
	s.runsMu.Lock()
	s.runs = append(s.runs, run)
	s.runsMu.Unlock()
}

func (s *buildIndexFieldLocalState) flushRegularInto(dst *buildIndexFieldState) {
	if s == nil || dst == nil {
		return
	}
	if s.numeric {
		if run := buildIndexFixedRunFromPostingMap(s.fixed); run.keyCount() > 0 {
			dst.appendRun(run)
		}
		return
	}
	if run := buildIndexStringRunFromPostingMap(s.vals); run.keyCount() > 0 {
		dst.appendRun(run)
	}
}

func (s *buildIndexFieldLocalState) flushAllInto(dst *buildIndexFieldState) {
	if s == nil || dst == nil {
		return
	}
	s.flushRegularInto(dst)
	if !s.nils.IsEmpty() {
		dst.nilMu.Lock()
		ids := dst.nils
		ids = ids.BuildMergedOwned(s.nils)
		dst.nils = ids
		dst.nilMu.Unlock()
		s.nils = posting.List{}
	}
	if len(s.lenMap) > 0 {
		dst.lenMu.Lock()
		for ln, ids := range s.lenMap {
			merged := dst.lenMap[ln]
			merged = merged.BuildMergedOwned(ids)
			dst.lenMap[ln] = merged
		}
		dst.lenMu.Unlock()
		clear(s.lenMap)
	}
}

func (s *buildIndexFieldLocalState) release() {
	if s == nil {
		return
	}
	posting.ClearMapOwned(s.vals)
	s.vals = nil
	posting.ClearMapOwned(s.fixed)
	s.fixed = nil
	posting.ClearMapOwned(s.lenMap)
	s.lenMap = nil
	s.nils.Release()
	s.nils = posting.List{}
}

func (h buildIndexRunHeap) Len() int { return len(h.buf) }

func (h buildIndexRunHeap) less(i, j int) bool {
	return compareIndexKeys(h.buf[i].key, h.buf[j].key) < 0
}

func (h buildIndexRunHeap) swap(i, j int) {
	h.buf[i], h.buf[j] = h.buf[j], h.buf[i]
}

func (h *buildIndexRunHeap) init() {
	for i := len(h.buf)/2 - 1; i >= 0; i-- {
		h.down(i)
	}
}

func (h *buildIndexRunHeap) up(pos int) {
	for pos > 0 {
		parent := (pos - 1) >> 1
		if !h.less(pos, parent) {
			return
		}
		h.swap(parent, pos)
		pos = parent
	}
}

func (h *buildIndexRunHeap) down(pos int) {
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

func (h *buildIndexRunHeap) push(run, pos int) {
	cur := h.runs[run]
	if pos < 0 || pos >= cur.keyCount() {
		return
	}
	h.buf = append(h.buf, buildIndexRunCursor{
		run: run,
		pos: pos,
		key: cur.keyAt(pos),
	})
	h.up(len(h.buf) - 1)
}

func (h *buildIndexRunHeap) pop() buildIndexRunCursor {
	item := h.buf[0]
	last := len(h.buf) - 1
	h.buf[0] = h.buf[last]
	h.buf = h.buf[:last]
	if len(h.buf) > 0 {
		h.down(0)
	}
	return item
}

func (s *buildIndexFieldState) materializeStorage() fieldIndexStorage {
	if s == nil || len(s.runs) == 0 {
		return fieldIndexStorage{}
	}
	runs := s.runs
	s.runs = nil
	defer releaseBuildIndexRuns(runs)
	total := 0
	numeric := s.numeric
	for i := range runs {
		total += runs[i].keyCount()
	}
	if total == 0 {
		return fieldIndexStorage{}
	}

	h := buildIndexRunHeap{
		runs: runs,
		buf:  make([]buildIndexRunCursor, 0, len(runs)),
	}
	for i := range runs {
		run := runs[i]
		if run.keyCount() == 0 {
			continue
		}
		h.buf = append(h.buf, buildIndexRunCursor{
			run: i,
			pos: 0,
			key: run.keyAt(0),
		})
	}
	if len(h.buf) == 0 {
		return fieldIndexStorage{}
	}
	h.init()

	entries := make([]index, 0, min(total, fieldIndexChunkThreshold))
	var builder fieldIndexChunkBuilder
	var stream fieldIndexChunkStreamBuilder
	chunked := false

	for h.Len() > 0 {
		item := h.pop()
		run := &h.runs[item.run]
		key := item.key
		merged := run.takePosting(item.pos)
		h.push(item.run, item.pos+1)

		for h.Len() > 0 {
			next := h.buf[0]
			if compareIndexKeys(next.key, key) != 0 {
				break
			}
			item = h.pop()
			run = &h.runs[item.run]
			merged = merged.BuildMergedOwned(run.takePosting(item.pos))
			h.push(item.run, item.pos+1)
		}
		merged = merged.BuildOptimized()
		if !chunked {
			entries = append(entries, index{Key: key, IDs: merged})
			if len(entries) >= fieldIndexChunkThreshold {
				builder = newFieldIndexChunkBuilder(total)
				stream = newFieldIndexChunkStreamBuilder(&builder, numeric)
				for i := range entries {
					stream.append(entries[i].Key, entries[i].IDs)
				}
				entries = nil
				chunked = true
			}
			continue
		}
		stream.append(key, merged)
	}

	if !chunked {
		if len(entries) == 0 {
			return fieldIndexStorage{}
		}
		return newFlatFieldIndexStorage(&entries)
	}

	stream.finish()
	return newChunkedFieldIndexStorage(builder.root())
}

func (s *buildIndexFieldState) release() {
	if s == nil {
		return
	}
	for i := range s.runs {
		s.runs[i].release()
	}
	s.runs = nil
	s.nils.Release()
	s.nils = posting.List{}
	posting.ClearMapOwned(s.lenMap)
	s.lenMap = nil
}

func (s *buildIndexFieldState) materializeNilStorage() fieldIndexStorage {
	if s == nil {
		return fieldIndexStorage{}
	}
	ids := s.nils
	s.nils = posting.List{}
	ids = ids.BuildOptimized()
	if ids.IsEmpty() {
		return fieldIndexStorage{}
	}
	entries := []index{{
		Key: indexKeyFromStoredString(nilIndexEntryKey, false),
		IDs: ids,
	}}
	return newFlatFieldIndexStorage(&entries)
}

func (s *buildIndexFieldState) materializeLenStorage(universe posting.List) (fieldIndexStorage, bool) {
	if s == nil {
		return fieldIndexStorage{}, false
	}
	lenMap := s.lenMap
	s.lenMap = nil
	return materializeLenFieldStorageOwned(universe, lenMap)
}

func materializeLenFieldStorageOwned(universe posting.List, lenMap map[uint32]posting.List) (fieldIndexStorage, bool) {
	if len(lenMap) == 0 || universe.IsEmpty() {
		return fieldIndexStorage{}, false
	}
	empty := lenMap[0]
	emptyCard := empty.Cardinality()
	universeCard := universe.Cardinality()
	nonEmptyCard := uint64(0)
	if universeCard > emptyCard {
		nonEmptyCard = universeCard - emptyCard
	}

	useZeroComplement := nonEmptyCard > 0 && nonEmptyCard < emptyCard
	entries := make([]index, 0, len(lenMap)+1)
	for ln, ids := range lenMap {
		if useZeroComplement && ln == 0 {
			continue
		}
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			continue
		}
		entries = append(entries, index{
			Key: indexKeyFromU64(uint64(ln)),
			IDs: ids,
		})
	}
	if useZeroComplement {
		nonEmptyPosting := universe.Clone()
		if !empty.IsEmpty() {
			nonEmptyPosting = nonEmptyPosting.BuildAndNot(empty)
		}
		nonEmptyPosting = nonEmptyPosting.BuildOptimized()
		if !nonEmptyPosting.IsEmpty() {
			entries = append(entries, index{
				Key: indexKeyFromString(lenIndexNonEmptyKey),
				IDs: nonEmptyPosting,
			})
		}
	}
	if len(entries) == 0 {
		return fieldIndexStorage{}, false
	}

	slices.SortFunc(entries, func(a, b index) int {
		return compareIndexKeys(a.Key, b.Key)
	})
	return newFlatFieldIndexStorage(&entries), useZeroComplement
}

func (db *DB[K, V]) buildIndex(skipFields map[string]struct{}) error {
	if skipFields == nil {
		skipFields = map[string]struct{}{}
	}

	type buildField struct {
		acc     indexedFieldAccessor
		slice   bool
		numeric bool
	}

	active := make([]buildField, 0, len(db.indexedFieldAccess))
	for _, acc := range db.indexedFieldAccess {
		if _, skip := skipFields[acc.name]; skip {
			continue
		}
		active = append(active, buildField{
			acc:     acc,
			slice:   acc.field.Slice,
			numeric: acc.field.KeyKind == fieldWriteKeysOrderedU64,
		})
	}

	fcnt := len(active)
	if fcnt <= 0 {
		if !db.lenIndexLoaded {
			db.buildLenIndex()
		}
		db.lenIndexLoaded = false
		return nil
	}

	start := time.Now()

	type rawdata struct {
		v   []byte
		idx uint64
	}

	fieldStates := make([]*buildIndexFieldState, len(active))
	for i := range active {
		fieldStates[i] = newBuildIndexFieldState(active[i].numeric, active[i].slice)
	}

	jobs := make(chan rawdata, 10000)

	workers := runtime.NumCPU()
	workerErrs := make([]error, workers)

	localUniverse := make([]posting.List, workers)
	buildOK := false
	defer cleanupBuildIndexFailure(&buildOK, fieldStates, localUniverse)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(widx int) {
			defer wg.Done()

			lu := localUniverse[widx]
			lu.Release()
			lu = posting.List{}
			localStates := make([]buildIndexFieldLocalState, len(active))
			for i := range active {
				localStates[i] = newBuildIndexFieldLocalState(active[i].numeric, active[i].slice)
			}
			defer releaseBuildIndexLocalStates(localStates)

			var zero V
			for kv := range jobs {
				val, err := db.decode(kv.v)
				if err != nil {
					localUniverse[widx] = lu
					workerErrs[widx] = err
					cancel()
					return
				}
				ptr := unsafe.Pointer(val)
				idx := kv.idx

				lu = lu.BuildAdded(idx)

				for k := range active {
					active[k].acc.writeBuild(ptr, buildFieldWriteSink{state: &localStates[k], idx: idx})
					if localStates[k].shouldFlushRegular() {
						localStates[k].flushRegularInto(fieldStates[k])
					}
				}
				*val = zero
				db.recPool.Put(val)
			}
			for i := range localStates {
				localStates[i].flushAllInto(fieldStates[i])
			}
			localUniverse[widx] = lu
		}(i)
	}

	err := db.bolt.View(func(tx *bbolt.Tx) error {

		defer wg.Wait()
		defer close(jobs)

		b := tx.Bucket(db.bucket)
		if b == nil {
			return nil
		}
		done := ctx.Done()
		c := b.Cursor()
		if db.strkey {
			db.strmap.Lock()
			defer db.strmap.Unlock()
		}
		nextGCAt := uint64(indexBuildGCStride)
		scanned := uint64(0)
		for k, v := c.First(); k != nil; k, v = c.Next() {
			idx := db.idxFromKeyNoLock(k)
			select {
			case <-done:
				return nil
			case jobs <- rawdata{v: v, idx: idx}:
			}
			scanned++
			if scanned >= nextGCAt {
				forceMemoryCleanup(false)
				nextGCAt += indexBuildGCStride
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("scan error: %w", err)
	}
	for _, err = range workerErrs {
		if err != nil {
			return fmt.Errorf("scan error: %w", err)
		}
	}

	if db.index == nil {
		db.index = make(map[string]fieldIndexStorage)
	}
	if db.nilIndex == nil {
		db.nilIndex = make(map[string]fieldIndexStorage)
	}
	if db.lenIndex == nil {
		db.lenIndex = make(map[string]fieldIndexStorage)
	}
	if db.lenZeroComplement == nil {
		db.lenZeroComplement = make(map[string]bool)
	}

	prevUniverse := db.universe
	db.universe = posting.List{}
	for i := range localUniverse {
		if localUniverse[i].IsEmpty() {
			continue
		}
		db.universe = db.universe.BuildMergedOwned(localUniverse[i])
	}

	for i := range fieldStates {
		name := active[i].acc.name
		oldIndexStorage := db.index[name]
		if storage := fieldStates[i].materializeStorage(); storage.keyCount() > 0 {
			releaseFieldIndexStorageOwned(oldIndexStorage)
			db.index[name] = storage
		} else {
			releaseFieldIndexStorageOwned(oldIndexStorage)
			delete(db.index, name)
		}
		oldNilStorage := db.nilIndex[name]
		if storage := fieldStates[i].materializeNilStorage(); storage.keyCount() > 0 {
			releaseFieldIndexStorageOwned(oldNilStorage)
			db.nilIndex[name] = storage
		} else {
			releaseFieldIndexStorageOwned(oldNilStorage)
			delete(db.nilIndex, name)
		}
		oldLenStorage := db.lenIndex[name]
		if storage, useZeroComplement := fieldStates[i].materializeLenStorage(db.universe); active[i].slice {
			if storage.keyCount() > 0 {
				releaseFieldIndexStorageOwned(oldLenStorage)
				db.lenIndex[name] = storage
			} else {
				releaseFieldIndexStorageOwned(oldLenStorage)
				delete(db.lenIndex, name)
			}
			if useZeroComplement {
				db.lenZeroComplement[name] = true
			} else {
				delete(db.lenZeroComplement, name)
			}
		}
	}

	recordCount := db.universe.Cardinality()

	db.stats.BuildTime = time.Since(start)
	db.stats.BuildRPS = int(float64(recordCount) / max(time.Since(start).Seconds(), 1))

	for name, f := range db.fields {
		if f.Slice {
			if _, ok := db.lenIndex[name]; !ok {
				db.buildLenIndex()
				break
			}
		}
	}
	db.lenIndexLoaded = false
	if err = db.publishCurrentSequenceSnapshotNoLock(); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	prevUniverse.Release()

	active = nil
	fieldStates = nil
	localUniverse = nil
	workerErrs = nil
	forceMemoryCleanup(true)
	buildOK = true

	return nil
}

func buildInt64Key(v int64) uint64 {
	return uint64(v) ^ (uint64(1) << 63)
}

func buildFloat64Key(f float64) uint64 {
	return orderedFloat64Key(f)
}

func addDistinctStrings(n int, valueAt func(int) string, add func(string)) int {
	if n == 0 {
		return 0
	}
	if n == 1 {
		add(valueAt(0))
		return 1
	}
	seen := stringSetPool.Get(n)
	defer stringSetPool.Put(seen)
	distinct := 0
	for i := 0; i < n; i++ {
		cur := valueAt(i)
		if _, ok := seen[cur]; ok {
			continue
		}
		seen[cur] = struct{}{}
		distinct++
		add(cur)
	}
	return distinct
}

type buildFieldWriteSink struct {
	state *buildIndexFieldLocalState
	idx   uint64
}

func (s buildFieldWriteSink) setNil() {
	s.state.addNil(s.idx)
}

func (s buildFieldWriteSink) setLen(length int) {
	s.state.addLen(length, s.idx)
}

func (s buildFieldWriteSink) addString(key string) {
	s.state.addValue(key, s.idx)
}

func (s buildFieldWriteSink) addFixed(key uint64) {
	s.state.addFixedValue(key, s.idx)
}

func indexKeyFromStoredString(s string, fixed8 bool) indexKey {
	if fixed8 && len(s) == 8 {
		return indexKeyFromFixed8String(s)
	}
	return indexKeyFromString(s)
}

func (db *DB[K, V]) idxFromKeyNoLock(key []byte) uint64 {
	if db.strkey {
		return db.strmap.createIdxNoLock(string(key))
	}
	return binary.BigEndian.Uint64(key)
}

func (db *DB[K, V]) buildLenIndex() {
	releaseFieldIndexStorageMapOwned(db.lenIndex)
	db.lenIndex = make(map[string]fieldIndexStorage, len(db.fields))
	if db.lenZeroComplement == nil {
		db.lenZeroComplement = make(map[string]bool)
	} else {
		clear(db.lenZeroComplement)
	}

	for name, f := range db.fields {
		if !f.Slice {
			continue
		}
		result, useZeroComplement := rebuildLenIndexField(db.universe, newFieldOverlayStorage(db.index[name]))
		db.lenIndex[name] = newFlatFieldIndexStorage(result)
		if useZeroComplement {
			db.lenZeroComplement[name] = true
		}
	}
}

func (db *DB[K, V]) isLenZeroComplementField(field string) bool {
	if field == "" {
		return false
	}
	if db.traceRoot != nil {
		return db.lenZeroComplement[field]
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.lenZeroComplement[field]
}

const (
	initialIndexLen    = 32 << 10
	maxStoredStringLen = 64 << 30
	indexBuildGCStride = 100_000

	nilIndexEntryKey    = ""
	lenIndexNonEmptyKey = "\xFFNONEMPTY"
)

func (db *DB[K, V]) loadIndex() (skipFields map[string]struct{}, plannerStats *plannerStatsSnapshot, err error) {
	defer recoverLoadIndex(&err)

	f, err := os.Open(db.rbiFile)
	if err != nil {
		return nil, nil, err
	}
	defer closeFile(f)

	start := time.Now()
	reader := bufio.NewReaderSize(f, 32<<20)

	ver, err := reader.ReadByte()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: reading version: %w", errPersistedIndexInvalid, err)
	}

	var lenLoaded bool
	switch ver {
	case 24:
		skipFields, plannerStats, lenLoaded, err = db.loadIndexV24(reader)
	default:
		return nil, nil, fmt.Errorf("%w: unsupported persisted index version: %v", errPersistedIndexInvalid, ver)
	}
	if err != nil {
		if errors.Is(err, errPersistedIndexStale) || errors.Is(err, errPersistedIndexInvalid) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("error loading index: %w", err)
	}

	db.lenIndexLoaded = lenLoaded
	db.stats.LoadTime = time.Since(start)
	if err = db.publishCurrentSequenceSnapshotNoLock(); err != nil {
		return nil, nil, fmt.Errorf("publish snapshot: %w", err)
	}
	forceMemoryCleanup(true)

	return skipFields, plannerStats, nil
}

func (db *DB[K, V]) loadIndexV24(reader *bufio.Reader) (map[string]struct{}, *plannerStatsSnapshot, bool, error) {
	storedSeq, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, nil, false, fmt.Errorf("decode: reading bucket sequence: %w", err)
	}

	currentSeq, err := db.currentBucketSequence()
	if err != nil {
		return nil, nil, false, fmt.Errorf("decode: reading current bucket sequence: %w", err)
	}
	if storedSeq != currentSeq {
		return nil, nil, false, fmt.Errorf("%w: bucket sequence mismatch (stored=%v, current=%v)", errPersistedIndexStale, storedSeq, currentSeq)
	}

	skipFields, plannerStats, lenLoaded, err := db.loadIndexPayload(reader)
	if err != nil {
		return nil, nil, false, fmt.Errorf("%w: %w", errPersistedIndexInvalid, err)
	}
	return skipFields, plannerStats, lenLoaded, nil
}

func (db *DB[K, V]) loadIndexPayload(reader *bufio.Reader) (map[string]struct{}, *plannerStatsSnapshot, bool, error) {

	var universe posting.List
	universe, err := posting.ReadFrom(reader)
	if err != nil {
		return nil, nil, false, fmt.Errorf("decode: reading universe: %w", err)
	}

	strmap, err := readStrMap(reader, defaultSnapshotStrMapCompactDepth)
	if err != nil {
		return nil, nil, false, err
	}

	compatible, err := db.readFieldCompatibility(reader)
	if err != nil {
		return nil, nil, false, err
	}

	indexes, err := readIndexSections(reader, compatible)
	if err != nil {
		return nil, nil, false, fmt.Errorf("decode: reading index sections: %w", err)
	}

	nilIndexes, err := readIndexSections(reader, compatible)
	if err != nil {
		return nil, nil, false, fmt.Errorf("decode: reading nil index sections: %w", err)
	}

	lenIndexes, err := readIndexSections(reader, compatible)
	if err != nil {
		return nil, nil, false, fmt.Errorf("decode: reading len index sections: %w", err)
	}

	plannerStats, err := readPlannerStatsSnapshot(reader, compatible)
	if err != nil {
		return nil, nil, false, fmt.Errorf("decode: reading planner stats: %w", err)
	}

	skipFields := make(map[string]struct{}, len(db.fields))
	for name := range db.fields {
		_, hasRegular := indexes[name]
		_, hasNil := nilIndexes[name]
		if compatible[name] && (hasRegular || hasNil) {
			skipFields[name] = struct{}{}
		}
	}

	db.universe = universe
	db.strmap = strmap
	db.index = indexes
	db.nilIndex = nilIndexes
	db.lenIndex = lenIndexes
	db.lenZeroComplement = detectLenZeroComplement(lenIndexes)

	lenLoaded := len(skipFields) == len(db.fields)
	if lenLoaded && !universe.IsEmpty() {
		for name, f := range db.fields {
			if f == nil || !f.Slice {
				continue
			}
			if _, ok := skipFields[name]; !ok {
				continue
			}
			s, ok := lenIndexes[name]
			base := s.flatSlice()
			if !ok || base == nil || len(*base) == 0 {
				lenLoaded = false
				break
			}
		}
	}

	return skipFields, plannerStats, lenLoaded, nil
}

func detectLenZeroComplement(indexes map[string]fieldIndexStorage) map[string]bool {
	if len(indexes) == 0 {
		return make(map[string]bool)
	}
	out := make(map[string]bool, len(indexes))
	for f, storage := range indexes {
		slice := storage.flatSlice()
		if slice == nil || len(*slice) == 0 {
			continue
		}
		for _, ix := range *slice {
			if indexKeyEqualsString(ix.Key, lenIndexNonEmptyKey) {
				out[f] = true
				break
			}
		}
	}
	return out
}

func (db *DB[K, V]) readFieldCompatibility(reader *bufio.Reader) (map[string]bool, error) {
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading fields len: %w", err)
	}
	if count > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("decode: stored field count overflows int: %v", count)
	}
	compatible := make(map[string]bool, max(0, min(int(count), len(db.fields))))
	seen := make(map[string]struct{}, max(0, min(int(count), len(db.fields))))

	for i := uint64(0); i < count; i++ {
		name, stored, err := readField(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading field: %w", err)
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("decode: duplicate field %q", name)
		}
		seen[name] = struct{}{}
		cur := db.fields[name]
		if sameFieldDefinition(cur, stored) {
			compatible[name] = true
		}
	}
	return compatible, nil
}

func sameFieldDefinition(a, b *field) bool {
	if a == nil || b == nil {
		return false
	}
	if a.Unique != b.Unique ||
		a.Kind != b.Kind ||
		a.Ptr != b.Ptr ||
		a.Slice != b.Slice ||
		a.UseVI != b.UseVI ||
		a.DBName != b.DBName ||
		!slices.Equal(a.Index, b.Index) {
		return false
	}
	return true
}

func (db *DB[K, V]) storeIndex() error {
	if hook := db.testHooks.beforeStoreIndex; hook != nil {
		if err := hook(); err != nil {
			return err
		}
	}

	forceMemoryCleanup(true)

	tmpFile := db.rbiFile + ".temp"
	defer os.Remove(tmpFile)

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer closeFile(f)

	buf := bufio.NewWriterSize(f, 32<<20)

	seq, err := db.currentBucketSequence()
	if err != nil {
		return fmt.Errorf("store: reading bucket sequence: %w", err)
	}

	if err = db.storeIndexV24(buf, seq); err != nil {
		return err
	}

	if err = buf.Flush(); err != nil {
		return fmt.Errorf("flushing write buffers: %w", err)
	}
	if err = f.Sync(); err != nil {
		return fmt.Errorf("syncing persisted index temp file: %w", err)
	}

	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpFile, db.rbiFile); err != nil {
		return err
	}
	if err = syncDir(db.rbiFile); err != nil {
		return fmt.Errorf("syncing persisted index directory: %w", err)
	}
	return nil
}

func (db *DB[K, V]) storeIndexV24(writer *bufio.Writer, bucketSeq uint64) error {
	if err := writer.WriteByte(24); err != nil {
		return fmt.Errorf("store: writing version: %w", err)
	}
	if err := writeUvarint(writer, bucketSeq); err != nil {
		return fmt.Errorf("store: writing bucket sequence: %w", err)
	}

	return db.storeIndexPayload(writer)
}

func (db *DB[K, V]) storeIndexPayload(writer *bufio.Writer) error {
	snap := db.getSnapshot()
	universe := snap.universe.Borrow()

	if err := universe.WriteTo(writer); err != nil {
		return fmt.Errorf("encode: writing universe: %w", err)
	}

	if err := writeStrMapSnapshot(writer, snap.strmap); err != nil {
		return err
	}

	if err := writeFields(writer, db.fields); err != nil {
		return err
	}

	fieldNames := sortedFieldNames(snap.fieldNameSet())

	if err := writeIndexFamily(writer, fieldNames, func(field string) fieldIndexStorage {
		return snap.index[field]
	}); err != nil {
		return err
	}

	nilFieldNames := sortedFieldNames(snap.nilFieldNameSet())

	if err := writeIndexFamily(writer, nilFieldNames, func(field string) fieldIndexStorage {
		return snap.nilIndex[field]
	}); err != nil {
		return err
	}

	lenFieldNames := sortedFieldNames(snap.lenFieldNameSet())

	if err := writeIndexFamily(writer, lenFieldNames, func(field string) fieldIndexStorage {
		return snap.lenIndex[field]
	}); err != nil {
		return err
	}

	statsVersion := db.planner.statsVersion.Load()
	if statsVersion == 0 {
		statsVersion = 1
	} else {
		statsVersion++
	}
	if err := writePlannerStatsSnapshot(writer, db.plannerStatsSnapshotForPersistLocked(statsVersion)); err != nil {
		return err
	}

	return nil
}

func writeIndexFamily(writer *bufio.Writer, fields []string, storageFor func(string) fieldIndexStorage) error {
	if err := writeUvarint(writer, uint64(len(fields))); err != nil {
		return fmt.Errorf("encode: writing index family len: %w", err)
	}

	for _, f := range fields {
		if err := writeFieldIndexSection(writer, f, storageFor(f)); err != nil {
			return fmt.Errorf("encode: writing index field %q: %w", f, err)
		}
	}
	return nil
}

func writeFieldIndexSection(writer *bufio.Writer, field string, storage fieldIndexStorage) error {
	if err := writeString(writer, field); err != nil {
		return fmt.Errorf("encode: writing index field name: %w", err)
	}
	return writeFieldIndexStorage(writer, storage)
}

const (
	fieldStorageEncodingFlat    byte = 1
	fieldStorageEncodingChunked byte = 2

	fieldIndexChunkEncodingString byte = 1
	fieldIndexChunkEncodingRaw8   byte = 2

	indexKeyEncodingString byte = 1
	indexKeyEncodingRaw8   byte = 2

	strMapEncodingDense  byte = 1
	strMapEncodingSparse byte = 2
)

func writeFieldIndexStorage(writer *bufio.Writer, storage fieldIndexStorage) error {
	switch {
	case storage.chunked != nil:
		if err := writer.WriteByte(fieldStorageEncodingChunked); err != nil {
			return fmt.Errorf("encode: writing storage encoding: %w", err)
		}
		root := storage.chunked
		if err := writeUvarint(writer, uint64(len(root.pages))); err != nil {
			return fmt.Errorf("encode: writing page count: %w", err)
		}
		for i := range root.pages {
			page := root.pages[i]
			if err := writeUvarint(writer, uint64(len(page.refs))); err != nil {
				return fmt.Errorf("encode: writing page refs: %w", err)
			}
			for j := range page.refs {
				if err := writeFieldIndexChunk(writer, page.refs[j].chunk); err != nil {
					return err
				}
			}
		}
		return nil
	case storage.flat != nil:
		if err := writer.WriteByte(fieldStorageEncodingFlat); err != nil {
			return fmt.Errorf("encode: writing storage encoding: %w", err)
		}
		entries := storage.flat.entries
		if err := writeUvarint(writer, uint64(len(entries))); err != nil {
			return fmt.Errorf("encode: writing flat len: %w", err)
		}
		return writeIndexEntries(writer, entries)
	default:
		return fmt.Errorf("encode: empty field storage")
	}
}

func writeFieldIndexChunk(writer *bufio.Writer, chunk *fieldIndexChunk) error {
	if chunk == nil || chunk.keyCount() == 0 {
		return fmt.Errorf("encode: empty chunk")
	}
	if chunk.numeric != nil {
		if err := writer.WriteByte(fieldIndexChunkEncodingRaw8); err != nil {
			return fmt.Errorf("encode: writing chunk encoding: %w", err)
		}
		if err := writeUvarint(writer, uint64(len(chunk.numeric))); err != nil {
			return fmt.Errorf("encode: writing numeric chunk len: %w", err)
		}
		var buf [8]byte
		for i := range chunk.numeric {
			binary.BigEndian.PutUint64(buf[:], chunk.numeric[i])
			if _, err := writer.Write(buf[:]); err != nil {
				return fmt.Errorf("encode: writing numeric chunk key: %w", err)
			}
			if err := chunk.posts[i].WriteTo(writer); err != nil {
				return fmt.Errorf("encode: writing numeric chunk posting: %w", err)
			}
		}
		return nil
	}

	if err := writer.WriteByte(fieldIndexChunkEncodingString); err != nil {
		return fmt.Errorf("encode: writing chunk encoding: %w", err)
	}
	if err := writeUvarint(writer, uint64(len(chunk.stringRefs))); err != nil {
		return fmt.Errorf("encode: writing string chunk len: %w", err)
	}
	for i := range chunk.stringRefs {
		ref := chunk.stringRefs[i]
		if err := writeUvarint(writer, uint64(ref.len)); err != nil {
			return fmt.Errorf("encode: writing string chunk key len: %w", err)
		}
		if ref.len > 0 {
			start := int(ref.off)
			end := start + int(ref.len)
			if _, err := writer.Write(chunk.stringData[start:end]); err != nil {
				return fmt.Errorf("encode: writing string chunk key: %w", err)
			}
		}
		if err := chunk.posts[i].WriteTo(writer); err != nil {
			return fmt.Errorf("encode: writing string chunk posting: %w", err)
		}
	}
	return nil
}

func writeIndexEntries(writer *bufio.Writer, entries []index) error {
	for i := range entries {
		if err := writeIndexEntry(writer, entries[i]); err != nil {
			return err
		}
	}
	return nil
}

func writeIndexEntry(writer *bufio.Writer, entry index) error {
	if err := writeIndexKey(writer, entry.Key); err != nil {
		return fmt.Errorf("encode: writing entry key: %w", err)
	}
	if err := entry.IDs.WriteTo(writer); err != nil {
		return fmt.Errorf("encode: writing entry posting: %w", err)
	}
	return nil
}

func writeIndexKey(writer *bufio.Writer, key indexKey) error {
	if key.isNumeric() {
		if err := writer.WriteByte(indexKeyEncodingRaw8); err != nil {
			return err
		}
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], key.meta)
		_, err := writer.Write(b[:])
		return err
	}
	if err := writer.WriteByte(indexKeyEncodingString); err != nil {
		return err
	}
	return writeString(writer, key.asUnsafeString())
}

func writeFields(writer *bufio.Writer, fields map[string]*field) error {
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)

	if err := writeUvarint(writer, uint64(len(names))); err != nil {
		return fmt.Errorf("encode: writing fields len: %w", err)
	}
	for _, name := range names {
		if err := writeField(writer, name, fields[name]); err != nil {
			return fmt.Errorf("encode: writing field %q: %w", name, err)
		}
	}
	return nil
}

func writeField(writer *bufio.Writer, name string, f *field) error {
	if err := writeString(writer, name); err != nil {
		return err
	}
	if err := writeBool(writer, f.Unique); err != nil {
		return err
	}
	if err := writeUvarint(writer, uint64(f.Kind)); err != nil {
		return err
	}
	if err := writeBool(writer, f.Ptr); err != nil {
		return err
	}
	if err := writeBool(writer, f.Slice); err != nil {
		return err
	}
	if err := writeBool(writer, f.UseVI); err != nil {
		return err
	}
	if err := writeString(writer, f.DBName); err != nil {
		return err
	}
	if err := writeUvarint(writer, uint64(len(f.Index))); err != nil {
		return err
	}
	for _, idx := range f.Index {
		if idx < 0 {
			return fmt.Errorf("negative field index")
		}
		if err := writeUvarint(writer, uint64(idx)); err != nil {
			return err
		}
	}
	return nil
}

func readField(reader *bufio.Reader) (string, *field, error) {
	name, err := readString(reader)
	if err != nil {
		return "", nil, err
	}
	unique, err := readBool(reader)
	if err != nil {
		return "", nil, err
	}
	kind, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", nil, err
	}
	ptr, err := readBool(reader)
	if err != nil {
		return "", nil, err
	}
	slice, err := readBool(reader)
	if err != nil {
		return "", nil, err
	}
	useVI, err := readBool(reader)
	if err != nil {
		return "", nil, err
	}
	dbName, err := readString(reader)
	if err != nil {
		return "", nil, err
	}
	valIndexLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", nil, err
	}
	valIndex := make([]int, 0, valIndexLen)
	for i := uint64(0); i < valIndexLen; i++ {
		v, err := binary.ReadUvarint(reader)
		if err != nil {
			return "", nil, err
		}
		if v > uint64(^uint(0)>>1) {
			return "", nil, fmt.Errorf("field index element overflows int")
		}
		valIndex = append(valIndex, int(v))
	}
	return name, &field{
		Unique: unique,
		Kind:   reflect.Kind(kind),
		Ptr:    ptr,
		Slice:  slice,
		UseVI:  useVI,
		DBName: dbName,
		Index:  valIndex,
	}, nil
}

func writeStrMapSnapshot(writer *bufio.Writer, sm *strMapSnapshot) error {
	if sm == nil {
		if err := writeUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing strmap next: %w", err)
		}
		if err := writer.WriteByte(strMapEncodingSparse); err != nil {
			return fmt.Errorf("encode: writing strmap encoding: %w", err)
		}
		return writeUvarint(writer, 0)
	}

	if err := writeUvarint(writer, sm.Next); err != nil {
		return fmt.Errorf("encode: writing strmap next: %w", err)
	}

	var chainInline [32]strMapSnapshotPersistNode
	chain, usedCount := strMapSnapshotPersistNodes(sm, chainInline[:0])

	if strMapSnapshotShouldPersistSparse(sm, usedCount) {
		if err := writer.WriteByte(strMapEncodingSparse); err != nil {
			return fmt.Errorf("encode: writing strmap encoding: %w", err)
		}
		if err := writeUvarint(writer, uint64(usedCount)); err != nil {
			return fmt.Errorf("encode: writing strmap sparse len: %w", err)
		}
		it := strMapSnapshotPersistIter{chain: chain}
		for {
			idx, value, ok := it.next()
			if !ok {
				break
			}
			if err := writeUvarint(writer, idx); err != nil {
				return fmt.Errorf("encode: writing strmap sparse idx: %w", err)
			}
			if err := writeString(writer, value); err != nil {
				return fmt.Errorf("encode: writing strmap sparse string: %w", err)
			}
		}
		return nil
	}

	if err := writer.WriteByte(strMapEncodingDense); err != nil {
		return fmt.Errorf("encode: writing strmap encoding: %w", err)
	}

	denseLen := int(sm.Next) + 1
	if err := writeUvarint(writer, uint64(denseLen)); err != nil {
		return fmt.Errorf("encode: writing strmap dense len: %w", err)
	}

	flagIter := strMapSnapshotPersistIter{chain: chain}
	idx, _, ok := flagIter.next()
	for base := 0; base < denseLen; base += 8 {
		baseIdx := uint64(base)
		limit := baseIdx + 8
		var flags byte
		for ok && idx < limit {
			flags |= 1 << uint(idx-baseIdx)
			idx, _, ok = flagIter.next()
		}
		if err := writer.WriteByte(flags); err != nil {
			return fmt.Errorf("encode: writing strmap flags: %w", err)
		}
	}

	valueIter := strMapSnapshotPersistIter{chain: chain}
	for {
		_, value, ok := valueIter.next()
		if !ok {
			break
		}
		if err := writeString(writer, value); err != nil {
			return fmt.Errorf("encode: writing strmap string: %w", err)
		}
	}
	return nil
}

type strMapSnapshotPersistNode struct {
	start     uint64
	next      uint64
	strs      map[uint64]string
	denseStrs []string
	denseUsed []bool
}

func strMapSnapshotPersistNodes(sm *strMapSnapshot, inline []strMapSnapshotPersistNode) ([]strMapSnapshotPersistNode, int) {
	if sm == nil {
		return nil, 0
	}
	if len(sm.readDirs) > 0 {
		var nodes []strMapSnapshotPersistNode
		usedCount := 0
		pageCount := strMapReadPageCount(sm.Next)
		for page := 0; page < pageCount; page++ {
			readPage := sm.readPageAtNoLock(page)
			if readPage == nil {
				continue
			}
			nodes = append(nodes, strMapSnapshotPersistNode{
				start:     readPage.Start,
				next:      readPage.Next,
				strs:      readPage.Strs,
				denseStrs: readPage.DenseStrs,
				denseUsed: readPage.DenseUsed,
			})
			usedCount += readPage.usedCountNoLock()
		}
		if len(nodes) == 0 {
			return nil, usedCount
		}
		if len(nodes) <= cap(inline) {
			copy(inline[:len(nodes)], nodes)
			nodes = inline[:len(nodes)]
		}
		return nodes, usedCount
	}

	depth := 0
	usedCount := 0
	for cur := sm; cur != nil; cur = cur.base {
		depth++
		usedCount += strMapSnapshotOwnUsedCount(cur)
	}
	if depth == 0 {
		return nil, 0
	}

	var nodes []strMapSnapshotPersistNode
	if depth <= cap(inline) {
		nodes = inline[:depth]
	} else {
		nodes = make([]strMapSnapshotPersistNode, depth)
	}
	i := depth
	for cur := sm; cur != nil; cur = cur.base {
		i--
		start := cur.baseNextNoLock() + 1
		if cur.base == nil {
			start = 1
		}
		denseStrs, denseUsed := strMapDenseWindowNoLock(cur.DenseStrs, cur.DenseUsed, start, cur.Next)
		nodes[i] = strMapSnapshotPersistNode{
			start:     start,
			next:      cur.Next,
			strs:      cur.Strs,
			denseStrs: denseStrs,
			denseUsed: denseUsed,
		}
	}
	return nodes, usedCount
}

type strMapSnapshotPersistIter struct {
	chain      []strMapSnapshotPersistNode
	node       int
	mode       byte
	densePos   int
	denseLimit int
	sparse     []strMapSparseEntry
	sparsePos  int
}

func (it *strMapSnapshotPersistIter) next() (uint64, string, bool) {
	for it.node < len(it.chain) {
		switch it.mode {
		case 1:
			node := it.chain[it.node]
			for it.densePos < it.denseLimit {
				idx := uint64(it.densePos)
				it.densePos++
				pos := int(idx - node.start)
				if pos < 0 || pos >= len(node.denseUsed) || !node.denseUsed[pos] {
					continue
				}
				return idx, node.denseStrs[pos], true
			}
			it.mode = 0
			it.node++
			continue
		case 2:
			if it.sparsePos < len(it.sparse) {
				entry := it.sparse[it.sparsePos]
				it.sparsePos++
				return entry.idx, entry.value, true
			}
			it.sparse = it.sparse[:0]
			it.sparsePos = 0
			it.mode = 0
			it.node++
			continue
		}

		node := it.chain[it.node]
		if len(node.denseStrs) > 0 || len(node.denseUsed) > 0 {
			start := int(node.start)
			limit := start + min(len(node.denseStrs), len(node.denseUsed))
			if start >= limit {
				it.node++
				continue
			}
			it.densePos = start
			it.denseLimit = limit
			it.mode = 1
			continue
		}
		if len(node.strs) == 0 {
			it.node++
			continue
		}

		it.sparse = it.sparse[:0]
		for idx, value := range node.strs {
			if idx < node.start || idx > node.next {
				continue
			}
			it.sparse = append(it.sparse, strMapSparseEntry{idx: idx, value: value})
		}
		slices.SortFunc(it.sparse, func(a, b strMapSparseEntry) int {
			switch {
			case a.idx < b.idx:
				return -1
			case a.idx > b.idx:
				return 1
			default:
				return 0
			}
		})
		it.mode = 2
	}
	return 0, "", false
}

func readStrMap(reader *bufio.Reader, compactAt int) (*strMapper, error) {
	next, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading strmap next: %w", err)
	}
	enc, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("decode: reading strmap encoding: %w", err)
	}
	switch enc {
	case strMapEncodingDense:
		denseLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading strmap dense len: %w", err)
		}
		if denseLen == 0 {
			return nil, fmt.Errorf("decode: invalid zero strmap dense len")
		}
		if denseLen > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("decode: strmap dense len overflows int: %v", denseLen)
		}
		if next >= denseLen {
			return nil, fmt.Errorf("decode: strmap next out of range: next=%v denseLen=%v", next, denseLen)
		}

		flags := make([]byte, (int(denseLen)+7)>>3)
		if _, err := io.ReadFull(reader, flags); err != nil {
			return nil, fmt.Errorf("decode: reading strmap flags: %w", err)
		}

		usedCount := 0
		for _, b := range flags {
			for x := b; x != 0; x &= x - 1 {
				usedCount++
			}
		}

		strs := make([]string, int(denseLen))
		used := make([]bool, int(denseLen))
		keys := make(map[string]uint64, max(0, usedCount-1))

		for i := 0; i < int(denseLen); i++ {
			if flags[i>>3]&(1<<uint(i&7)) == 0 {
				continue
			}
			s, err := readString(reader)
			if err != nil {
				return nil, fmt.Errorf("decode: reading strmap string: %w", err)
			}
			strs[i] = s
			used[i] = true
			if i > 0 {
				keys[s] = uint64(i)
			}
		}

		strmap := newStrMapper(uint64(max(0, usedCount-1)), compactAt)
		strmap.replaceAllDenseNoLock(keys, strs, used, next)
		return strmap, nil

	case strMapEncodingSparse:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading strmap sparse len: %w", err)
		}
		if count > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("decode: strmap sparse len overflows int: %v", count)
		}
		keys := make(map[string]uint64, int(count))
		strs := make(map[uint64]string, int(count))
		for i := uint64(0); i < count; i++ {
			idx, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, fmt.Errorf("decode: reading strmap sparse idx: %w", err)
			}
			if idx == 0 || idx > next {
				return nil, fmt.Errorf("decode: strmap sparse idx out of range: idx=%v next=%v", idx, next)
			}
			if _, exists := strs[idx]; exists {
				return nil, fmt.Errorf("decode: duplicate strmap sparse idx: %v", idx)
			}
			s, err := readString(reader)
			if err != nil {
				return nil, fmt.Errorf("decode: reading strmap sparse string: %w", err)
			}
			if _, exists := keys[s]; exists {
				return nil, fmt.Errorf("decode: duplicate strmap sparse key: %q", s)
			}
			keys[s] = idx
			strs[idx] = s
		}
		strmap := newStrMapper(count, compactAt)
		strmap.replaceAllSparseNoLock(keys, strs, next)
		return strmap, nil

	default:
		return nil, fmt.Errorf("decode: invalid strmap encoding %v", enc)
	}
}

type strMapSparseEntry struct {
	idx   uint64
	value string
}

func strMapSnapshotShouldPersistSparse(sm *strMapSnapshot, usedCount int) bool {
	if sm == nil {
		return false
	}
	if sm.Next > uint64(^uint(0)>>1) {
		return true
	}
	denseLen := max(len(sm.DenseStrs), len(sm.DenseUsed))
	if denseLen <= 1 {
		denseLen = int(sm.Next) + 1
	}
	if denseLen <= 1 {
		return false
	}
	if usedCount == 0 {
		return false
	}
	return estimateSparseStrMapReverseBytes(usedCount) < estimateDenseStrMapReverseBytes(denseLen)
}

func strMapSnapshotOwnUsedCount(sm *strMapSnapshot) int {
	if sm == nil {
		return 0
	}
	if len(sm.Keys) > 0 {
		return len(sm.Keys)
	}
	if sm.Strs != nil {
		return len(sm.Strs)
	}
	limit := min(len(sm.DenseStrs), len(sm.DenseUsed))
	if limit <= 1 {
		return 0
	}
	count := 0
	for i := 1; i < limit; i++ {
		if sm.DenseUsed[i] {
			count++
		}
	}
	return count
}

func estimateDenseStrMapReverseBytes(denseLen int) uint64 {
	if denseLen <= 0 {
		return 0
	}
	return uint64(denseLen) * uint64(unsafe.Sizeof("")+unsafe.Sizeof(false))
}

func estimateSparseStrMapReverseBytes(usedCount int) uint64 {
	if usedCount <= 0 {
		return 0
	}
	const sparseMapLoadNumerator = 2
	const sparseMapLoadDenominator = 13 // ceil(n / 6.5) == ceil(2n / 13)
	buckets := (usedCount*sparseMapLoadNumerator + sparseMapLoadDenominator - 1) / sparseMapLoadDenominator
	if buckets < 1 {
		buckets = 1
	}
	bucketSize := uint64(8 + 8*unsafe.Sizeof(uint64(0)) + 8*unsafe.Sizeof("") + unsafe.Sizeof(uintptr(0)))
	return bucketSize * uint64(buckets)
}

func writePlannerStatsSnapshot(writer *bufio.Writer, s *plannerStatsSnapshot) error {
	if s == nil {
		if err := writeUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing planner stats version: %w", err)
		}
		if err := writeUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing planner stats generated_at: %w", err)
		}
		if err := writeUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing planner stats universe: %w", err)
		}
		return writeUvarint(writer, 0)
	}

	if err := writeUvarint(writer, s.Version); err != nil {
		return fmt.Errorf("encode: writing planner stats version: %w", err)
	}

	generatedAt := uint64(0)
	if !s.GeneratedAt.IsZero() {
		generatedAt = uint64(s.GeneratedAt.UnixNano())
	}
	if err := writeUvarint(writer, generatedAt); err != nil {
		return fmt.Errorf("encode: writing planner stats generated_at: %w", err)
	}
	if err := writeUvarint(writer, s.UniverseCardinality); err != nil {
		return fmt.Errorf("encode: writing planner stats universe: %w", err)
	}

	fields := sortedMapFieldNames(s.Fields)
	if err := writeUvarint(writer, uint64(len(fields))); err != nil {
		return fmt.Errorf("encode: writing planner stats field count: %w", err)
	}
	for _, f := range fields {
		if err := writeString(writer, f); err != nil {
			return fmt.Errorf("encode: writing planner stats field name: %w", err)
		}
		if err := writePlannerFieldStats(writer, s.Fields[f]); err != nil {
			return fmt.Errorf("encode: writing planner stats field %q: %w", f, err)
		}
	}
	return nil
}

func readPlannerStatsSnapshot(reader *bufio.Reader, compatible map[string]bool) (*plannerStatsSnapshot, error) {
	version, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats version: %w", err)
	}
	generatedAtNanos, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats generated_at: %w", err)
	}
	universe, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats universe: %w", err)
	}
	fieldCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats field count: %w", err)
	}
	if fieldCount > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("decode: planner stats field count overflows int: %v", fieldCount)
	}
	if version == 0 && generatedAtNanos == 0 && universe == 0 && fieldCount == 0 {
		return nil, nil
	}

	fields := make(map[string]PlannerFieldStats, min(int(fieldCount), len(compatible)))
	for i := uint64(0); i < fieldCount; i++ {
		f, err := readString(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading planner stats field name: %w", err)
		}
		stats, err := readPlannerFieldStats(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading planner stats field %q: %w", f, err)
		}
		if compatible[f] {
			if _, exists := fields[f]; exists {
				return nil, fmt.Errorf("decode: duplicate planner stats field %q", f)
			}
			fields[f] = stats
		}
	}
	if version == 0 && generatedAtNanos == 0 && universe == 0 && len(fields) == 0 {
		return nil, nil
	}
	for f := range compatible {
		if _, ok := fields[f]; !ok {
			return nil, fmt.Errorf("decode: missing planner stats field %q", f)
		}
	}

	out := &plannerStatsSnapshot{
		Version:             version,
		UniverseCardinality: universe,
		Fields:              fields,
	}
	if generatedAtNanos > 0 {
		out.GeneratedAt = time.Unix(0, int64(generatedAtNanos)).UTC()
	}
	return out, nil
}

func writePlannerFieldStats(writer *bufio.Writer, s PlannerFieldStats) error {
	if err := writeUvarint(writer, s.DistinctKeys); err != nil {
		return err
	}
	if err := writeUvarint(writer, s.NonEmptyKeys); err != nil {
		return err
	}
	if err := writeUvarint(writer, s.TotalBucketCard); err != nil {
		return err
	}
	if err := writeUvarint(writer, s.MaxBucketCard); err != nil {
		return err
	}
	if err := writeUvarint(writer, s.P50BucketCard); err != nil {
		return err
	}
	if err := writeUvarint(writer, s.P95BucketCard); err != nil {
		return err
	}
	return nil
}

func readPlannerFieldStats(reader *bufio.Reader) (PlannerFieldStats, error) {
	distinct, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}
	nonEmpty, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}
	total, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}
	maxCard, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}
	p50, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}
	p95, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}

	out := PlannerFieldStats{
		DistinctKeys:    distinct,
		NonEmptyKeys:    nonEmpty,
		TotalBucketCard: total,
		MaxBucketCard:   maxCard,
		P50BucketCard:   p50,
		P95BucketCard:   p95,
	}
	if distinct > 0 {
		out.AvgBucketCard = float64(total) / float64(distinct)
	}
	return out, nil
}

func sortedMapFieldNames[T any](m map[string]T) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for f := range m {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func readIndexSections(
	reader *bufio.Reader,
	compatible map[string]bool,
) (map[string]fieldIndexStorage, error) {
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return make(map[string]fieldIndexStorage), nil
	}

	out := make(map[string]fieldIndexStorage, min(int(count), len(compatible)))
	seen := make(map[string]struct{}, min(int(count), len(compatible)))
	for i := uint64(0); i < count; i++ {
		f, err := readString(reader)
		if err != nil {
			return nil, fmt.Errorf("reading field name: %w", err)
		}
		if _, exists := seen[f]; exists {
			return nil, fmt.Errorf("duplicate field %q", f)
		}
		seen[f] = struct{}{}

		keep := compatible[f]
		storage, err := readFieldIndexStorage(reader, keep)
		if err != nil {
			return nil, fmt.Errorf("reading field storage: %w", err)
		}
		if keep && storage.keyCount() > 0 {
			out[f] = storage
		}
	}

	return out, nil
}

func readFieldIndexStorage(reader *bufio.Reader, keep bool) (fieldIndexStorage, error) {
	tag, err := reader.ReadByte()
	if err != nil {
		return fieldIndexStorage{}, err
	}
	switch tag {
	case fieldStorageEncodingFlat:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return fieldIndexStorage{}, fmt.Errorf("reading flat len: %w", err)
		}
		if !keep {
			for i := uint64(0); i < count; i++ {
				if err := skipIndexEntry(reader); err != nil {
					return fieldIndexStorage{}, err
				}
			}
			return fieldIndexStorage{}, nil
		}
		entries := make([]index, 0, count)
		for i := uint64(0); i < count; i++ {
			ent, err := readIndexEntry(reader)
			if err != nil {
				return fieldIndexStorage{}, err
			}
			if ent.IDs.IsEmpty() {
				continue
			}
			entries = append(entries, ent)
		}
		return newFlatFieldIndexStorage(&entries), nil

	case fieldStorageEncodingChunked:
		pageCount, err := binary.ReadUvarint(reader)
		if err != nil {
			return fieldIndexStorage{}, fmt.Errorf("reading page count: %w", err)
		}
		if !keep {
			for i := uint64(0); i < pageCount; i++ {
				refCount, err := binary.ReadUvarint(reader)
				if err != nil {
					return fieldIndexStorage{}, fmt.Errorf("reading page refs: %w", err)
				}
				for j := uint64(0); j < refCount; j++ {
					if err := skipFieldIndexChunk(reader); err != nil {
						return fieldIndexStorage{}, err
					}
				}
			}
			return fieldIndexStorage{}, nil
		}
		builder := newFieldIndexChunkBuilder(0)
		for i := uint64(0); i < pageCount; i++ {
			refCount, err := binary.ReadUvarint(reader)
			if err != nil {
				return fieldIndexStorage{}, fmt.Errorf("reading page refs: %w", err)
			}
			refs := make([]fieldIndexChunkRef, 0, refCount)
			for j := uint64(0); j < refCount; j++ {
				chunk, err := readFieldIndexChunk(reader)
				if err != nil {
					return fieldIndexStorage{}, err
				}
				if chunk == nil || chunk.keyCount() == 0 {
					continue
				}
				last := chunk.keyCount() - 1
				refs = append(refs, fieldIndexChunkRef{
					last:  chunk.keyAt(last),
					chunk: chunk,
				})
			}
			if len(refs) > 0 {
				builder.appendOwnedPage(newFieldIndexChunkDirPage(refs))
			}
		}
		return newChunkedFieldIndexStorage(builder.root()), nil

	default:
		return fieldIndexStorage{}, fmt.Errorf("invalid field storage encoding %v", tag)
	}
}

func readFieldIndexChunk(reader *bufio.Reader) (*fieldIndexChunk, error) {
	tag, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	switch tag {
	case fieldIndexChunkEncodingRaw8:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("reading numeric chunk len: %w", err)
		}
		if count == 0 {
			return nil, nil
		}
		if count > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("numeric chunk len overflows int: %v", count)
		}
		keys := make([]uint64, int(count))
		posts := make([]posting.List, int(count))
		var buf [8]byte
		for i := range keys {
			if _, err := io.ReadFull(reader, buf[:]); err != nil {
				return nil, fmt.Errorf("reading numeric chunk key: %w", err)
			}
			keys[i] = binary.BigEndian.Uint64(buf[:])
			ids, err := posting.ReadFrom(reader)
			if err != nil {
				return nil, fmt.Errorf("reading numeric chunk posting: %w", err)
			}
			posts[i] = ids
		}
		return newNumericFieldIndexChunk(posts, keys, postingRows(posts)), nil

	case fieldIndexChunkEncodingString:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("reading string chunk len: %w", err)
		}
		if count == 0 {
			return nil, nil
		}
		if count > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("string chunk len overflows int: %v", count)
		}
		refs := make([]fieldIndexStringRef, int(count))
		posts := make([]posting.List, int(count))
		data := make([]byte, 0, int(count)*8)
		for i := range refs {
			n, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, fmt.Errorf("reading string chunk key len: %w", err)
			}
			if n > uint64(^uint(0)>>1) {
				return nil, fmt.Errorf("string chunk key len overflows int: %v", n)
			}
			keyLen := int(n)
			start := len(data)
			data = slices.Grow(data, keyLen)
			data = data[:start+keyLen]
			if keyLen > 0 {
				if _, err := io.ReadFull(reader, data[start:start+keyLen]); err != nil {
					return nil, fmt.Errorf("reading string chunk key: %w", err)
				}
			}
			refs[i] = fieldIndexStringRef{off: uint32(start), len: uint32(keyLen)}
			ids, err := posting.ReadFrom(reader)
			if err != nil {
				return nil, fmt.Errorf("reading string chunk posting: %w", err)
			}
			posts[i] = ids
		}
		return newStringFieldIndexChunk(posts, refs, data, postingRows(posts)), nil

	default:
		return nil, fmt.Errorf("invalid field chunk encoding %v", tag)
	}
}

func skipFieldIndexChunk(reader *bufio.Reader) error {
	tag, err := reader.ReadByte()
	if err != nil {
		return err
	}
	switch tag {
	case fieldIndexChunkEncodingRaw8:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return fmt.Errorf("reading numeric chunk len: %w", err)
		}
		for i := uint64(0); i < count; i++ {
			if _, err := io.CopyN(io.Discard, reader, 8); err != nil {
				return err
			}
			if err := posting.Skip(reader); err != nil {
				return err
			}
		}
		return nil

	case fieldIndexChunkEncodingString:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return fmt.Errorf("reading string chunk len: %w", err)
		}
		for i := uint64(0); i < count; i++ {
			n, err := binary.ReadUvarint(reader)
			if err != nil {
				return fmt.Errorf("reading string chunk key len: %w", err)
			}
			if n > 0 {
				if _, err := io.CopyN(io.Discard, reader, int64(n)); err != nil {
					return err
				}
			}
			if err := posting.Skip(reader); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("invalid field chunk encoding %v", tag)
	}
}

func readIndexEntry(reader *bufio.Reader) (index, error) {
	key, err := readIndexKey(reader)
	if err != nil {
		return index{}, err
	}
	ids, err := posting.ReadFrom(reader)
	if err != nil {
		return index{}, err
	}
	return index{Key: key, IDs: ids}, nil
}

func skipIndexEntry(reader *bufio.Reader) error {
	if err := skipIndexKey(reader); err != nil {
		return err
	}
	return posting.Skip(reader)
}

func readIndexKey(reader *bufio.Reader) (indexKey, error) {
	tag, err := reader.ReadByte()
	if err != nil {
		return indexKey{}, err
	}
	switch tag {
	case indexKeyEncodingString:
		s, err := readString(reader)
		if err != nil {
			return indexKey{}, err
		}
		return indexKeyFromString(s), nil
	case indexKeyEncodingRaw8:
		var buf [8]byte
		if _, err := io.ReadFull(reader, buf[:]); err != nil {
			return indexKey{}, err
		}
		return indexKeyFromU64(binary.BigEndian.Uint64(buf[:])), nil
	default:
		return indexKey{}, fmt.Errorf("invalid index key encoding %v", tag)
	}
}

func skipIndexKey(reader *bufio.Reader) error {
	tag, err := reader.ReadByte()
	if err != nil {
		return err
	}
	switch tag {
	case indexKeyEncodingString:
		return skipString(reader)
	case indexKeyEncodingRaw8:
		_, err := io.CopyN(io.Discard, reader, 8)
		return err
	default:
		return fmt.Errorf("invalid index key encoding %v", tag)
	}
}

func sortedFieldNames(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for f := range set {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func writeUvarint(writer *bufio.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	if _, err := writer.Write(buf[:n]); err != nil {
		return err
	}
	return nil
}

func writeBool(writer *bufio.Writer, v bool) error {
	if v {
		return writer.WriteByte(1)
	}
	return writer.WriteByte(0)
}

func readBool(reader *bufio.Reader) (bool, error) {
	v, err := reader.ReadByte()
	if v != 0 && v != 1 {
		return false, fmt.Errorf("corrupted bool value: %v", v)
	}
	return v > 0, err
}

func writeString(writer *bufio.Writer, s string) error {
	if err := writeUvarint(writer, uint64(len(s))); err != nil {
		return err
	}
	if len(s) == 0 {
		return nil
	}
	if _, err := io.WriteString(writer, s); err != nil {
		return err
	}
	return nil
}

func readString(reader *bufio.Reader) (string, error) {
	n, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	if n > maxStoredStringLen {
		return "", fmt.Errorf("string len %v exceeds limit (%v)", n, maxStoredStringLen)
	}
	if n > uint64(^uint(0)>>1) {
		return "", fmt.Errorf("string len %v overflows int", n)
	}
	b := make([]byte, int(n))
	if _, err = io.ReadFull(reader, b); err != nil {
		return "", err
	}
	s := unsafe.String(unsafe.SliceData(b), len(b))
	return s, nil
}

func skipString(reader *bufio.Reader) error {
	n, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if n > maxStoredStringLen {
		return fmt.Errorf("string len %v exceeds limit (%v)", n, maxStoredStringLen)
	}
	if n > uint64(^uint(0)>>1) {
		return fmt.Errorf("string len %v overflows int", n)
	}
	if _, err = io.CopyN(io.Discard, reader, int64(n)); err != nil {
		return err
	}
	return nil
}

// fieldOverlay provides read helpers over one immutable sorted index slice.
type fieldOverlay struct {
	base    []index
	chunked *fieldIndexChunkedRoot
}

type overlayRange struct {
	baseStart int
	baseEnd   int

	startPos fieldIndexChunkPos
	endPos   fieldIndexChunkPos
}

type overlayKeyCursor struct {
	base    []index
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

func newFieldOverlay(base *[]index) fieldOverlay {
	if base == nil {
		return fieldOverlay{}
	}
	return fieldOverlay{base: *base}
}

func newFieldOverlayStorage(storage fieldIndexStorage) fieldOverlay {
	if storage.chunked != nil {
		return fieldOverlay{chunked: storage.chunked}
	}
	return newFieldOverlay(storage.flatSlice())
}

func (o fieldOverlay) hasData() bool {
	return o.keyCount() > 0
}

func (o fieldOverlay) keyCount() int {
	if o.chunked != nil {
		return o.chunked.keyCount
	}
	return len(o.base)
}

func (o fieldOverlay) lowerBound(key string) int {
	if o.chunked != nil {
		return o.chunked.lowerBound(key)
	}
	return lowerBoundIndex(o.base, key)
}

func (o fieldOverlay) lowerBoundKey(key indexKey) int {
	if o.chunked != nil {
		_, rank := o.chunked.lowerBoundPosKey(key)
		return rank
	}
	return lowerBoundIndexKey(o.base, key)
}

func (o fieldOverlay) upperBound(key string) int {
	if o.chunked != nil {
		return o.chunked.upperBound(key)
	}
	return upperBoundIndex(o.base, key)
}

func (o fieldOverlay) upperBoundKey(key indexKey) int {
	if o.chunked != nil {
		_, rank := o.chunked.upperBoundPosKey(key)
		return rank
	}
	return upperBoundIndexKey(o.base, key)
}

func (o fieldOverlay) prefixRangeEnd(prefix string, start int) int {
	if o.chunked != nil {
		return o.chunked.prefixRangeEnd(prefix, start)
	}
	return prefixRangeEndIndex(o.base, prefix, start)
}

func (o fieldOverlay) lookupCardinality(key string) uint64 {
	ids := o.lookupPostingRetained(key)
	return ids.Cardinality()
}

func (o fieldOverlay) lookupPostingRetained(key string) posting.List {
	if o.chunked != nil {
		return o.chunked.lookupPostingRetained(key)
	}
	if len(o.base) == 0 {
		return posting.List{}
	}
	i := lowerBoundIndex(o.base, key)
	if i >= len(o.base) || !indexKeyEqualsString(o.base[i].Key, key) {
		return posting.List{}
	}
	return o.base[i].IDs.Borrow()
}

func (o fieldOverlay) postingAt(rank int) posting.List {
	if rank < 0 || rank >= o.keyCount() {
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

func (o fieldOverlay) lookupPostings(keys stringKeyReader) (*pooled.SliceBuf[posting.List], uint64) {
	postsBuf := postingSlicePool.Get()
	keyCount := stringKeyReaderLen(keys)
	postsBuf.Grow(keyCount)
	var est uint64

	for i := 0; i < keyCount; i++ {
		ids := o.lookupPostingRetained(keys.Get(i))
		if ids.IsEmpty() {
			continue
		}
		postsBuf.Append(ids)
		est += ids.Cardinality()
	}
	return postsBuf, est
}

func (o fieldOverlay) rangeForBounds(b rangeBounds) overlayRange {
	if o.chunked != nil {
		return o.rangeForBoundsChunked(b)
	}

	br := overlayRange{
		baseStart: 0,
		baseEnd:   o.keyCount(),
	}
	if b.empty {
		br.baseEnd = 0
		return br
	}

	if b.hasPrefix {
		ps := o.lowerBound(b.prefix)
		pe := o.prefixRangeEnd(b.prefix, ps)
		br.baseStart = max(br.baseStart, ps)
		br.baseEnd = min(br.baseEnd, pe)
	}

	if b.hasLo {
		bl := 0
		if b.loNumeric {
			bl = o.lowerBoundKey(b.loIndex)
		} else {
			bl = o.lowerBound(b.loKey)
		}
		if !b.loInc {
			if b.loNumeric {
				if bl < len(o.base) && compareIndexKeys(o.base[bl].Key, b.loIndex) == 0 {
					bl++
				}
			} else {
				ids := o.lookupPostingRetained(b.loKey)
				if !ids.IsEmpty() {
					bl++
				}
			}
		}
		br.baseStart = max(br.baseStart, bl)
	}

	if b.hasHi {
		var bh int
		if b.hiNumeric {
			if b.hiInc {
				bh = o.upperBoundKey(b.hiIndex)
			} else {
				bh = o.lowerBoundKey(b.hiIndex)
			}
		} else {
			if b.hiInc {
				bh = o.upperBound(b.hiKey)
			} else {
				bh = o.lowerBound(b.hiKey)
			}
		}
		br.baseEnd = min(br.baseEnd, bh)
	}

	if br.baseStart < 0 {
		br.baseStart = 0
	}
	if maxEnd := o.keyCount(); br.baseEnd > maxEnd {
		br.baseEnd = maxEnd
	}
	if br.baseStart > br.baseEnd {
		br.baseStart = br.baseEnd
	}

	return br
}

func (o fieldOverlay) rangeForBoundsChunked(b rangeBounds) overlayRange {
	br := overlayRange{
		baseStart: 0,
		baseEnd:   o.keyCount(),
	}
	if o.chunked == nil {
		return br
	}
	br.startPos = o.chunked.startPos()
	br.endPos = o.chunked.endPos()
	if b.empty {
		br.baseEnd = 0
		br.endPos = br.startPos
		return br
	}

	if b.hasPrefix {
		ps, psRank := o.chunked.lowerBoundPos(b.prefix)
		pe, peRank := o.chunked.prefixRangeEndPos(b.prefix, ps, psRank)
		if psRank > br.baseStart {
			br.baseStart = psRank
			br.startPos = ps
		}
		if peRank < br.baseEnd {
			br.baseEnd = peRank
			br.endPos = pe
		}
	}

	if b.hasLo {
		var (
			bl     fieldIndexChunkPos
			blRank int
		)
		if b.loNumeric {
			bl, blRank = o.chunked.lowerBoundPosKey(b.loIndex)
		} else {
			bl, blRank = o.chunked.lowerBoundPos(b.loKey)
		}
		if !b.loInc {
			if key, ok := o.chunked.posKey(bl); ok &&
				((b.loNumeric && compareIndexKeys(key, b.loIndex) == 0) ||
					(!b.loNumeric && indexKeyEqualsString(key, b.loKey))) {
				bl = o.chunked.advancePos(bl)
				blRank++
			}
		}
		if blRank > br.baseStart {
			br.baseStart = blRank
			br.startPos = bl
		}
	}

	if b.hasHi {
		var (
			bh     fieldIndexChunkPos
			bhRank int
		)
		if b.hiNumeric {
			if b.hiInc {
				bh, bhRank = o.chunked.upperBoundPosKey(b.hiIndex)
			} else {
				bh, bhRank = o.chunked.lowerBoundPosKey(b.hiIndex)
			}
		} else {
			if b.hiInc {
				bh, bhRank = o.chunked.upperBoundPos(b.hiKey)
			} else {
				bh, bhRank = o.chunked.lowerBoundPos(b.hiKey)
			}
		}
		if bhRank < br.baseEnd {
			br.baseEnd = bhRank
			br.endPos = bh
		}
	}

	if br.baseStart < 0 {
		br.baseStart = 0
		br.startPos = o.chunked.startPos()
	}
	if maxEnd := o.keyCount(); br.baseEnd > maxEnd {
		br.baseEnd = maxEnd
		br.endPos = o.chunked.endPos()
	}
	if br.baseStart > br.baseEnd {
		br.baseStart = br.baseEnd
		br.startPos = br.endPos
	}
	return br
}

func (o fieldOverlay) rangeByRanks(start, end int) overlayRange {
	if start < 0 {
		start = 0
	}
	if maxEnd := o.keyCount(); end > maxEnd {
		end = maxEnd
	}
	if start > end {
		start = end
	}
	br := overlayRange{
		baseStart: start,
		baseEnd:   end,
	}
	if o.chunked != nil {
		br.startPos = o.chunked.posForRank(start)
		br.endPos = o.chunked.posForRank(end)
	}
	return br
}

func (o fieldOverlay) newCursor(br overlayRange, desc bool) overlayKeyCursor {
	c := overlayKeyCursor{
		base:    o.base,
		chunked: o.chunked,
		desc:    desc,
	}
	if o.chunked != nil {
		c.remaining = br.baseEnd - br.baseStart
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
		c.pos = br.baseEnd - 1
		c.end = br.baseStart
		return c
	}
	c.pos = br.baseStart
	c.end = br.baseEnd
	return c
}

func (c *overlayKeyCursor) next() (indexKey, posting.List, bool) {
	if c.desc {
		if c.chunked != nil {
			if c.remaining <= 0 || c.chunk == nil {
				return indexKey{}, posting.List{}, false
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
							return indexKey{}, posting.List{}, false
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
			return indexKey{}, posting.List{}, false
		}
		ent := c.base[c.pos]
		c.pos--
		return ent.Key, ent.IDs, true
	}
	if c.chunked != nil {
		if c.remaining <= 0 || c.chunk == nil {
			return indexKey{}, posting.List{}, false
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
						return indexKey{}, posting.List{}, false
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
		return indexKey{}, posting.List{}, false
	}
	ent := c.base[c.pos]
	c.pos++
	return ent.Key, ent.IDs, true
}
