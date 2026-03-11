package rbi

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"go.etcd.io/bbolt"
)

func (db *DB[K, V]) buildIndex(skipFields map[string]struct{}) error {
	if skipFields == nil {
		skipFields = map[string]struct{}{}
	}

	type buildField struct {
		name   string
		index  []int
		getter valueGetterFn
		slice  bool
		fixed8 bool
	}

	active := make([]buildField, 0, len(db.fields))
	for name, f := range db.fields {
		if _, skip := skipFields[name]; skip {
			continue
		}
		getter, err := db.makeFieldValueGetter(f)
		if err != nil {
			return fmt.Errorf("build: field %q getter: %w", name, err)
		}
		active = append(active, buildField{
			name:   name,
			index:  f.Index,
			getter: getter,
			slice:  f.Slice,
			fixed8: fieldUsesFixed8Keys(f),
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

	type (
		rawdata struct {
			v   []byte
			idx uint64
		}
		interIndex []map[string]postingList // field index -> value -> ids
	)

	global := make(interIndex, len(active))
	for i := range active {
		global[i] = make(map[string]postingList, initialIndexLen)
	}

	jobs := make(chan rawdata, 10000)

	workers := runtime.NumCPU()
	locals := make([]interIndex, workers)
	workerErrs := make([]error, workers)

	localUniverse := make([]*roaring64.Bitmap, workers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(widx int) {
			defer wg.Done()

			local := make(interIndex, len(active))
			locals[widx] = local

			for k := range active {
				local[k] = make(map[string]postingList, 1024)
			}

			lu := roaring64.NewBitmap()
			localUniverse[widx] = lu

			var zero V
			for kv := range jobs {
				val, err := db.decode(kv.v)
				if err != nil {
					workerErrs[widx] = err
					cancel()
					return
				}
				ptr := unsafe.Pointer(val)
				idx := kv.idx
				rv := reflect.NewAt(db.vtype, ptr).Elem()
				if !rv.IsValid() {
					*val = zero
					db.recPool.Put(val)
					continue
				}

				lu.Add(idx)

				for k, f := range active {
					single, multi, ok := f.getter(rv.FieldByIndex(f.index))
					if !ok {
						continue
					}
					if f.slice {
						for _, s := range multi {
							addTo(local[k], s, idx)
						}
					} else {
						addTo(local[k], single, idx)
					}
				}
				*val = zero
				db.recPool.Put(val)
			}
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
		for k, v := c.First(); k != nil; k, v = c.Next() {
			idx := db.idxFromKeyNoLock(k)
			select {
			case <-done:
				return nil
			case jobs <- rawdata{v: v, idx: idx}:
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

	for _, local := range locals {
		if local == nil {
			continue
		}
		for i, fmap := range local {
			gmap := global[i]
			for val, lp := range fmap {
				gp := gmap[val]
				gp.OrPosting(lp)
				gmap[val] = gp
			}
		}
	}

	if db.index == nil {
		db.index = make(map[string]*[]index)
	}

	for i, values := range global {
		fixed8 := active[i].fixed8
		s := make([]index, 0, len(values))
		for val, ids := range values {
			ids.RunOptimizeAdaptive()
			if ids.IsEmpty() {
				continue
			}
			s = append(s, index{
				Key: indexKeyFromStoredString(val, fixed8),
				IDs: ids,
			})
		}
		slices.SortFunc(s, func(a, b index) int {
			return compareIndexKeys(a.Key, b.Key)
		})
		db.index[active[i].name] = &s
	}

	db.universe = roaring64.NewBitmap()
	for _, lu := range localUniverse {
		if lu != nil {
			db.universe.Or(lu)
		}
	}
	recordCount := db.universe.GetCardinality()

	db.stats.Index.BuildTime = time.Since(start)
	db.stats.Index.BuildRPS = int(float64(recordCount) / max(time.Since(start).Seconds(), 1))

	db.buildLenIndex()
	db.lenIndexLoaded = false
	db.publishSnapshotNoLock(db.currentBoltTxID())

	return nil
}

func addTo(m map[string]postingList, key string, id uint64) {
	p := m[key]
	p.Add(id)
	m[key] = p
}

func fieldUsesFixed8Keys(f *field) bool {
	if f == nil || f.UseVI {
		return false
	}
	switch f.Kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
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

func (db *DB[K, V]) keyFromIdx(idx uint64) (K, bool) {
	if db.strkey {
		v, ok := db.strMapSnapshot().getStringNoLock(idx)
		return *(*K)(unsafe.Pointer(&v)), ok
	}
	return *(*K)(unsafe.Pointer(&idx)), true
}

func (db *DB[K, V]) idFromIdxNoLock(idx uint64) K {
	if db.strkey {
		v, ok := db.strMapSnapshot().getStringNoLock(idx)
		if !ok {
			panic(fmt.Errorf("no id associated with idx %v", idx))
		}
		return *(*K)(unsafe.Pointer(&v))
	}
	return *(*K)(unsafe.Pointer(&idx))
}

func (db *DB[K, V]) buildLenIndex() {
	if db.universe == nil || db.universe.IsEmpty() {
		if db.lenZeroComplement == nil {
			db.lenZeroComplement = make(map[string]bool)
		} else {
			clear(db.lenZeroComplement)
		}
		return
	}

	if db.lenIndex == nil {
		db.lenIndex = make(map[string]*[]index)
	}
	if db.lenZeroComplement == nil {
		db.lenZeroComplement = make(map[string]bool)
	} else {
		clear(db.lenZeroComplement)
	}

	nonEmpty := roaring64.NewBitmap()

	for name, f := range db.fields {
		if !f.Slice {
			continue
		}

		slice := db.index[name]
		if slice == nil {
			continue
		}

		counts := make(map[uint64]uint32, 1024)

		nonEmpty.Clear()

		for _, ix := range *slice {
			ix.IDs.OrInto(nonEmpty)
			ix.IDs.ForEach(func(idx uint64) bool {
				counts[idx]++
				return true
			})
		}

		lenMap := make(map[uint32]postingList, len(counts)+1)

		for idx, ln := range counts {
			if ln == 0 {
				continue
			}
			p := lenMap[ln]
			p.Add(idx)
			lenMap[ln] = p
		}

		empty := db.universe.Clone()
		empty.AndNot(nonEmpty)
		useZeroComplement := false
		var nonEmptyPosting postingList
		if !empty.IsEmpty() {
			emptyCard := empty.GetCardinality()
			nonEmptyCard := nonEmpty.GetCardinality()
			if nonEmptyCard > 0 && nonEmptyCard < emptyCard {
				nonEmptyPosting = postingFromBitmapOwned(nonEmpty.Clone())
				useZeroComplement = !nonEmptyPosting.IsEmpty()
			}
		}
		if useZeroComplement {
			db.lenZeroComplement[name] = true
		} else if !empty.IsEmpty() {
			lenMap[0] = postingFromBitmapViewAdaptive(empty)
		}

		resultCap := len(lenMap)
		if useZeroComplement {
			resultCap++
		}
		result := make([]index, 0, resultCap)
		for ln, ids := range lenMap {
			ids.RunOptimizeAdaptive()
			if ids.IsEmpty() {
				continue
			}
			result = append(result, index{
				Key: indexKeyFromU64(uint64(ln)), // big-endian
				IDs: ids,
			})
		}
		if useZeroComplement {
			nonEmptyPosting.RunOptimizeAdaptive()
			if !nonEmptyPosting.IsEmpty() {
				result = append(result, index{
					Key: indexKeyFromString(lenIndexNonEmptyKey),
					IDs: nonEmptyPosting,
				})
			}
		}

		slices.SortFunc(result, func(a, b index) int {
			return compareIndexKeys(a.Key, b.Key)
		})

		db.lenIndex[name] = &result
	}
}

func (db *DB[K, V]) isLenZeroComplementField(field string) bool {
	if field == "" {
		return false
	}
	root := db
	if db.traceRoot != nil {
		root = db.traceRoot
	}
	root.mu.RLock()
	defer root.mu.RUnlock()
	return root.lenZeroComplement[field]
}

func findIndex(a *[]index, key string) postingList {
	slice := *a
	lo, hi := 0, len(slice)
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeyString(slice[mid].Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(slice) && indexKeyEqualsString(slice[lo].Key, key) {
		return slice[lo].IDs
	}
	return postingList{}
}

const (
	initialIndexLen    = 32 << 10
	maxStoredStringLen = 64 << 30
	maxStoredAllocHint = 64 << 10

	lenIndexNonEmptyKey = "\xFFNONEMPTY"

	// Guard against pathological/sparse persisted strmap indices that could
	// trigger oversized slice allocations on load.
	maxStoredStrMapSparseFactor = 4
	maxStoredStrMapSparseSlack  = 1 << 20
)

func (db *DB[K, V]) loadIndex() (skipFields map[string]struct{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	f, err := os.Open(db.rbiFile)
	if err != nil {
		return nil, err
	}
	defer closeFile(f)

	start := time.Now()
	reader := bufio.NewReaderSize(f, 32<<20)

	ver, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("load: reading version: %w", err)
	}

	var lenLoaded bool
	switch ver {
	case 4:
		skipFields, lenLoaded, err = db.loadIndexV4(reader)
	default:
		return nil, fmt.Errorf("unsupported persisted index version: %v", ver)
	}
	if err != nil {
		return nil, fmt.Errorf("error loading index: %w", err)
	}

	db.lenIndexLoaded = lenLoaded
	db.stats.Index.LoadTime = time.Since(start)
	db.publishSnapshotNoLock(db.currentBoltTxID())

	return skipFields, nil
}

func (db *DB[K, V]) loadIndexV4(reader *bufio.Reader) (map[string]struct{}, bool, error) {
	storedSeq, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, false, fmt.Errorf("decode: reading bucket sequence: %w", err)
	}

	currentSeq, err := db.currentBucketSequence()
	if err != nil {
		return nil, false, fmt.Errorf("decode: reading current bucket sequence: %w", err)
	}
	if storedSeq != currentSeq {
		return nil, false, fmt.Errorf("%w: bucket sequence mismatch (stored=%v, current=%v)", errPersistedIndexStale, storedSeq, currentSeq)
	}

	return db.loadIndexPayload(reader)
}

func (db *DB[K, V]) loadIndexPayload(reader *bufio.Reader) (map[string]struct{}, bool, error) {

	universe, err := readBitmap(reader)
	if err != nil {
		return nil, false, fmt.Errorf("decode: reading universe: %w", err)
	}

	strmap, err := readStrMap(reader, defaultSnapshotStrMapCompactDepth)
	if err != nil {
		return nil, false, err
	}

	compatible, err := db.readFieldCompatibility(reader)
	if err != nil {
		return nil, false, err
	}

	indexFieldFixed8 := make(map[string]bool, len(db.fields))
	lenFieldFixed8 := make(map[string]bool, len(db.fields))
	for name, f := range db.fields {
		indexFieldFixed8[name] = fieldUsesFixed8Keys(f)
		// len-index keys are fixed-width uint64 with optional NONEMPTY sentinel.
		lenFieldFixed8[name] = true
	}

	indexes, err := readIndexSections(reader, compatible, indexFieldFixed8)
	if err != nil {
		return nil, false, fmt.Errorf("decode: reading index sections: %w", err)
	}

	lenIndexes, err := readIndexSections(reader, compatible, lenFieldFixed8)
	if err != nil {
		return nil, false, fmt.Errorf("decode: reading len index sections: %w", err)
	}

	skipFields := make(map[string]struct{})
	for name := range db.fields {
		if _, ok := indexes[name]; ok && compatible[name] {
			skipFields[name] = struct{}{}
		}
	}

	db.universe = universe
	db.strmap = strmap
	db.index = indexes
	db.lenIndex = lenIndexes
	db.lenZeroComplement = detectLenZeroComplement(lenIndexes)

	lenLoaded := len(skipFields) == len(db.fields)
	if lenLoaded && universe != nil && !universe.IsEmpty() {
		for name, f := range db.fields {
			if f == nil || !f.Slice {
				continue
			}
			if _, ok := skipFields[name]; !ok {
				continue
			}
			s, ok := lenIndexes[name]
			if !ok || s == nil || len(*s) == 0 {
				lenLoaded = false
				break
			}
		}
	}

	return skipFields, lenLoaded, nil
}

func detectLenZeroComplement(indexes map[string]*[]index) map[string]bool {
	if len(indexes) == 0 {
		return make(map[string]bool)
	}
	out := make(map[string]bool, len(indexes))
	for f, slice := range indexes {
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

	compat := make(map[string]bool, max(0, min(int(count), len(db.fields))))

	for i := uint64(0); i < count; i++ {
		name, stored, err := readField(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading field: %w", err)
		}
		cur := db.fields[name]
		if fieldsEqual(cur, stored) {
			compat[name] = true
		}
	}

	return compat, nil
}

func fieldsEqual(a, b *field) bool {
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

	tmpFile := db.rbiFile + ".temp"

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

	if err = db.storeIndexV4(buf, seq); err != nil {
		return err
	}

	if err = buf.Flush(); err != nil {
		return fmt.Errorf("flushing write buffers: %w", err)
	}

	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpFile, db.rbiFile)
}

func (db *DB[K, V]) storeIndexV4(writer *bufio.Writer, bucketSeq uint64) error {
	if err := writer.WriteByte(4); err != nil {
		return fmt.Errorf("store: writing version: %w", err)
	}
	if err := writeUvarint(writer, bucketSeq); err != nil {
		return fmt.Errorf("store: writing bucket sequence: %w", err)
	}

	return db.storeIndexPayload(writer)
}

func (db *DB[K, V]) storeIndexPayload(writer *bufio.Writer) error {
	snap := db.getSnapshot()
	universe := snapshotUniverseView(snap)

	if err := writeBitmap(writer, universe); err != nil {
		return fmt.Errorf("encode: writing universe: %w", err)
	}

	if err := writeStrMapSnapshot(writer, snap.strmap); err != nil {
		return err
	}

	if err := writeFields(writer, db.fields); err != nil {
		return err
	}

	scratch := getRoaringBuf()
	defer releaseRoaringBuf(scratch)

	fieldNames := sortedFieldNames(snap.fieldNameSet())

	if err := writeIndexFamily(writer, fieldNames, scratch, func(field string) fieldOverlay {
		return newFieldOverlay(snap.fieldIndexSlice(field), snap.fieldDelta(field))
	}); err != nil {
		return err
	}

	lenFieldNames := sortedFieldNames(snap.lenFieldNameSet())

	if err := writeIndexFamily(writer, lenFieldNames, scratch, func(field string) fieldOverlay {
		return newFieldOverlay(snap.lenFieldIndexSlice(field), snap.lenFieldDelta(field))
	}); err != nil {
		return err
	}

	return nil
}

func writeIndexFamily(writer *bufio.Writer, fields []string, scratch *roaring64.Bitmap, overlayFor func(string) fieldOverlay) error {
	if err := writeUvarint(writer, uint64(len(fields))); err != nil {
		return fmt.Errorf("encode: writing index family len: %w", err)
	}

	for _, f := range fields {
		ov := overlayFor(f)
		if err := writeFieldIndexSection(writer, f, ov, scratch); err != nil {
			return fmt.Errorf("encode: writing index field %q: %w", f, err)
		}
	}
	return nil
}

func writeFieldIndexSection(writer *bufio.Writer, field string, ov fieldOverlay, scratch *roaring64.Bitmap) error {
	if err := writeString(writer, field); err != nil {
		return fmt.Errorf("encode: writing index field name: %w", err)
	}

	br := ov.rangeForBounds(rangeBounds{has: true})
	hint := uint64(0)
	if br.baseStart < br.baseEnd || br.deltaStart < br.deltaEnd {
		hint = uint64((br.baseEnd - br.baseStart) + (br.deltaEnd - br.deltaStart))
	}
	if err := writeUvarint(writer, hint); err != nil {
		return fmt.Errorf("encode: writing index field hint: %w", err)
	}

	cursor := ov.newCursor(br, false)
	for {
		key, baseBM, de, ok := cursor.next()
		if !ok {
			break
		}
		bm, _ := composePostingOwned(baseBM, de, scratch)
		if bm == nil || bm.IsEmpty() {
			continue
		}
		if err := writer.WriteByte(1); err != nil {
			return fmt.Errorf("encode: writing entry marker: %w", err)
		}
		if err := writeIndexKey(writer, key); err != nil {
			return fmt.Errorf("encode: writing entry key: %w", err)
		}
		if err := writePostingFromBitmap(writer, bm); err != nil {
			return fmt.Errorf("encode: writing entry posting: %w", err)
		}
	}

	if err := writer.WriteByte(0); err != nil {
		return fmt.Errorf("encode: writing entry terminator: %w", err)
	}
	return nil
}

func writeIndexKey(writer *bufio.Writer, key indexKey) error {
	if key.isNumeric() {
		if err := writeUvarint(writer, 8); err != nil {
			return err
		}
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], key.meta)
		_, err := writer.Write(b[:])
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
	if f == nil {
		f = new(field)
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
			return fmt.Errorf("encode: writing strmap len: %w", err)
		}
		return nil
	}

	capHint := 4
	if sm.depth > 0 {
		capHint = sm.depth
	}
	layers := make([]*strMapSnapshot, 0, capHint)
	total := 0
	for cur := sm; cur != nil; cur = cur.base {
		layers = append(layers, cur)
		total += len(cur.Keys)
	}

	if err := writeUvarint(writer, uint64(total)); err != nil {
		return fmt.Errorf("encode: writing strmap len: %w", err)
	}

	for i := len(layers) - 1; i >= 0; i-- {
		cur := layers[i]
		for k, v := range cur.Keys {
			if err := writeString(writer, k); err != nil {
				return fmt.Errorf("encode: writing strmap key: %w", err)
			}
			if err := writeUvarint(writer, v); err != nil {
				return fmt.Errorf("encode: writing strmap value: %w", err)
			}
		}
	}
	return nil
}

func readStrMap(reader *bufio.Reader, compactAt int) (*strMapper, error) {
	mapCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading strmap len: %w", err)
	}

	strmap := newStrMapper(mapCount, compactAt)
	maxAllowedIdx := uint64(0)
	if mapCount > 0 {
		maxAllowedIdx = mapCount
		if maxAllowedIdx <= ^uint64(0)-maxStoredStrMapSparseSlack {
			maxAllowedIdx += maxStoredStrMapSparseSlack
		} else {
			maxAllowedIdx = ^uint64(0)
		}
		if mapCount <= ^uint64(0)/maxStoredStrMapSparseFactor {
			v := mapCount * maxStoredStrMapSparseFactor
			if v > maxAllowedIdx {
				maxAllowedIdx = v
			}
		} else {
			maxAllowedIdx = ^uint64(0)
		}
	}
	var maxIdx uint64
	for i := uint64(0); i < mapCount; i++ {
		k, err := readString(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading strmap key: %w", err)
		}
		v, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading strmap value: %w", err)
		}
		if mapCount > 0 && v > maxAllowedIdx {
			return nil, fmt.Errorf("decode: strmap index too sparse: idx=%v keys=%v maxAllowed=%v", v, mapCount, maxAllowedIdx)
		}
		if v > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("decode: strmap index overflows int: %v", v)
		}
		iv := int(v)
		if iv >= len(strmap.Strs) {
			grow := iv + 1 - len(strmap.Strs)
			strmap.Strs = append(strmap.Strs, make([]string, grow)...)
			strmap.strsUsed = append(strmap.strsUsed, make([]bool, grow)...)
		}
		strmap.Keys[k] = v
		strmap.Strs[iv] = k
		strmap.strsUsed[iv] = true
		if v > maxIdx {
			maxIdx = v
		}
	}
	trim := int(maxIdx) + 1
	if trim < 1 {
		trim = 1
	}
	if trim < len(strmap.Strs) {
		strmap.Strs = strmap.Strs[:trim]
	}
	if trim < len(strmap.strsUsed) {
		strmap.strsUsed = strmap.strsUsed[:trim]
	}
	strmap.replaceAllNoLock(strmap.Keys, strmap.Strs, strmap.strsUsed, maxIdx)
	return strmap, nil
}

func readIndexSections(reader *bufio.Reader, compatible map[string]bool, fixed8ByField map[string]bool) (map[string]*[]index, error) {
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return make(map[string]*[]index), nil
	}

	out := make(map[string]*[]index, min(int(count), len(compatible)))

LOOP:
	for i := uint64(0); i < count; i++ {
		f, err := readString(reader)
		if err != nil {
			return nil, fmt.Errorf("reading field name: %w", err)
		}
		hint, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("reading field hint: %w", err)
		}

		keep := compatible[f]
		var entries []index
		if keep {
			entries = make([]index, 0, min(hint, maxStoredAllocHint))
		}

		for {
			marker, err := reader.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("reading entry marker: %w", err)
			}
			switch marker {

			case 0:
				if keep {
					s := entries
					out[f] = &s
				}
				continue LOOP

			case 1:

			default:
				return nil, fmt.Errorf("invalid entry marker %v", marker)
			}

			if !keep {
				if err = skipString(reader); err != nil {
					return nil, fmt.Errorf("skipping entry key: %w", err)
				}
				if err = skipPosting(reader); err != nil {
					return nil, fmt.Errorf("skipping entry posting: %w", err)
				}
				continue
			}
			key, err := readString(reader)
			if err != nil {
				return nil, fmt.Errorf("reading entry key: %w", err)
			}
			ids, err := readPosting(reader)
			if err != nil {
				return nil, fmt.Errorf("reading entry posting: %w", err)
			}
			if ids.IsEmpty() {
				continue
			}
			entries = append(entries, index{
				Key: indexKeyFromStoredString(key, fixed8ByField[f]),
				IDs: ids,
			})
		}
	}

	return out, nil
}

func sortedFieldNames(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for field := range set {
		out = append(out, field)
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
	if s == nilValue {
		return nilValue, nil
	}
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

func writeBitmap(writer *bufio.Writer, bm *roaring64.Bitmap) error {
	if bm == nil {
		if err := writeUvarint(writer, 0); err != nil {
			return err
		}
		return nil
	}
	size := bm.GetSerializedSizeInBytes()
	if err := writeUvarint(writer, size); err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	n, err := bm.WriteTo(writer)
	if err != nil {
		return err
	}
	if uint64(n) != size {
		return fmt.Errorf("bitmap write size mismatch: wrote %v expected %v", n, size)
	}
	return nil
}

func readBitmap(reader *bufio.Reader) (bm *roaring64.Bitmap, err error) {
	size, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return getRoaringBuf(), nil
	}
	if size > (^uint64(0) >> 1) {
		return nil, fmt.Errorf("bitmap size overflows int64: %v", size)
	}
	bm = getRoaringBuf()
	bm.Clear()
	defer func() {
		if r := recover(); r != nil {
			releaseRoaringBuf(bm)
			bm = nil
			err = fmt.Errorf("corrupted roaring64 bitmap payload: %v", r)
		}
	}()
	n, err := bm.ReadFrom(reader)
	if err != nil {
		releaseRoaringBuf(bm)
		return nil, err
	}
	if uint64(n) != size {
		releaseRoaringBuf(bm)
		return nil, fmt.Errorf("bitmap read size mismatch: read %v, expected %v", n, size)
	}
	return bm, nil
}

func skipBitmap(reader *bufio.Reader) error {
	size, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	if size > (^uint64(0) >> 1) {
		return fmt.Errorf("bitmap size overflows int64: %v", size)
	}
	if _, err = io.CopyN(io.Discard, reader, int64(size)); err != nil {
		return err
	}
	return nil
}

const (
	postingEncodingSingleton byte = 1
	postingEncodingBitmap    byte = 2
)

func writePostingFromBitmap(writer *bufio.Writer, bm *roaring64.Bitmap) error {
	if bm == nil || bm.IsEmpty() {
		return fmt.Errorf("cannot encode empty posting")
	}
	if bm.GetCardinality() == 1 {
		if err := writer.WriteByte(postingEncodingSingleton); err != nil {
			return err
		}
		return writeUvarint(writer, bm.Minimum())
	}
	if err := writer.WriteByte(postingEncodingBitmap); err != nil {
		return err
	}
	return writeBitmap(writer, bm)
}

func readPosting(reader *bufio.Reader) (postingList, error) {
	tag, err := reader.ReadByte()
	if err != nil {
		return postingList{}, err
	}
	switch tag {
	case postingEncodingSingleton:
		id, err := binary.ReadUvarint(reader)
		if err != nil {
			return postingList{}, err
		}
		return postingList{bm: postingSingleFlag, single: id}, nil
	case postingEncodingBitmap:
		bm, err := readBitmap(reader)
		if err != nil {
			return postingList{}, err
		}
		if bm == nil || bm.IsEmpty() {
			if bm != nil {
				releaseRoaringBuf(bm)
			}
			return postingList{}, nil
		}
		return postingFromBitmapOwned(bm), nil
	default:
		return postingList{}, fmt.Errorf("invalid posting encoding tag %v", tag)
	}
}

func skipPosting(reader *bufio.Reader) error {
	tag, err := reader.ReadByte()
	if err != nil {
		return err
	}
	switch tag {
	case postingEncodingSingleton:
		_, err := binary.ReadUvarint(reader)
		return err
	case postingEncodingBitmap:
		return skipBitmap(reader)
	default:
		return fmt.Errorf("invalid posting encoding tag %v", tag)
	}
}

func snapshotUniverseView(snap *indexSnapshot) *roaring64.Bitmap {
	if snap == nil {
		return roaring64.NewBitmap()
	}
	hasAdd := snap.universeAdd != nil && !snap.universeAdd.IsEmpty()
	hasDel := snap.universeRem != nil && !snap.universeRem.IsEmpty()
	if !hasAdd && !hasDel {
		if snap.universe != nil {
			return snap.universe
		}
		return roaring64.NewBitmap()
	}

	out := roaring64.NewBitmap()
	if snap.universe != nil && !snap.universe.IsEmpty() {
		out.Or(snap.universe)
	}
	if hasAdd {
		out.Or(snap.universeAdd)
	}
	if hasDel {
		out.AndNot(snap.universeRem)
	}
	return out
}
