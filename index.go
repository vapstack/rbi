package rbi

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"go.etcd.io/bbolt"
)

type mapKV struct {
	S string
	V uint64
}

const initialIndexLen = 32768

func (db *DB[K, V]) loadIndex() (map[string]struct{}, error) {
	f, err := os.Open(db.rbiFile)
	if err != nil {
		return nil, err
	}
	defer closeFile(f)

	start := time.Now()

	dec := gob.NewDecoder(bufio.NewReader(f))

	var ver byte

	if err = dec.Decode(&ver); err != nil {
		return nil, fmt.Errorf("decode: reading version: %w", err)
	}
	if ver != 1 {
		return nil, fmt.Errorf("invalid version")
	}

	/**/

	if err = dec.Decode(&db.universe); err != nil {
		return nil, fmt.Errorf("decode: reading universe: %w", err)
	}

	/**/

	var mapCount uint64
	if err = dec.Decode(&mapCount); err != nil {
		return nil, fmt.Errorf("decode: reading strmap len: %w", err)
	}
	strmap := newStrMapper(mapCount)

	var maxIdx uint64
	for i := uint64(0); i < mapCount; i++ {
		var kv mapKV
		if err = dec.Decode(&kv); err != nil {
			return nil, fmt.Errorf("decode: reading strmap value: %w", err)
		}
		strmap.Keys[kv.S] = kv.V
		strmap.Strs[kv.V] = kv.S
		if kv.V > maxIdx {
			maxIdx = kv.V
		}
	}
	strmap.Next = maxIdx

	/**/

	storedFields := make(map[string]*field)
	if err = dec.Decode(&storedFields); err != nil {
		return nil, fmt.Errorf("decode: reading fields: %w", err)
	}

	var count uint64
	if err = dec.Decode(&count); err != nil {
		return nil, fmt.Errorf("decode: reading index len: %w", err)
	}

	storedIndex := make(map[string]*[]index, count)
	for i := uint64(0); i < count; i++ {
		var key string
		var value *[]index

		if err = dec.Decode(&key); err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("reading index key: unexpected EOF")
			}
			return nil, fmt.Errorf("decode: reading index key: %w", err)
		}

		if err = dec.Decode(&value); err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("reading index value: unexpected EOF")
			}
			return nil, fmt.Errorf("decode: reading index value: %w", err)
		}

		storedIndex[key] = value
	}

	/**/

	skipFields := make(map[string]struct{})
	indexes := make(map[string]*[]index)

	for name, cur := range db.fields {
		old, ok := storedFields[name]
		if !ok {
			continue
		}

		if fieldsEqual(cur, old) {
			if ix, ok := storedIndex[name]; ok {
				indexes[name] = ix
				skipFields[name] = struct{}{}
			}
		}
	}

	db.index = indexes
	db.strmap = strmap
	db.stats.IndexLoadTime = time.Since(start)

	return skipFields, nil
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

	tmpFile := db.rbiFile + ".temp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer closeFile(f)

	buf := bufio.NewWriter(f)
	enc := gob.NewEncoder(buf)

	/**/

	var ver byte = 1

	if err = enc.Encode(ver); err != nil {
		return fmt.Errorf("encode: writing version: %w", err)
	}

	if err = enc.Encode(db.universe); err != nil {
		return fmt.Errorf("encode: writing universe: %w", err)
	}

	/**/

	if err = enc.Encode(uint64(len(db.strmap.Keys))); err != nil {
		return fmt.Errorf("encode: writing strmap len: %w", err)
	}
	for k, v := range db.strmap.Keys {
		kv := mapKV{
			S: k,
			V: v,
		}
		if err = enc.Encode(&kv); err != nil {
			return fmt.Errorf("encode: writing strmap value: %w", err)
		}
	}

	/**/

	if err = enc.Encode(db.fields); err != nil {
		return fmt.Errorf("encode: writing fields: %w", err)
	}

	if err = enc.Encode(uint64(len(db.index))); err != nil {
		return fmt.Errorf("encode: writing index len: %w", err)
	}

	for k, v := range db.index {
		if err = enc.Encode(k); err != nil {
			return fmt.Errorf("encode: writing index key: %w", err)
		}
		clean := make([]index, 0, len(*v))
		for _, i := range *v {
			if !i.IDs.IsEmpty() {
				clean = append(clean, i)
			}
		}
		if err = enc.Encode(&clean); err != nil {
			return fmt.Errorf("encode: writing index value: %w", err)
		}
	}

	if err = buf.Flush(); err != nil {
		return fmt.Errorf("flushing write buffers: %w", err)
	}

	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpFile, db.rbiFile)
}

/**/

func (db *DB[K, V]) buildIndex(skipFields map[string]struct{}) error {
	if skipFields == nil {
		skipFields = map[string]struct{}{}
	}

	fcnt := len(db.fields) - len(skipFields)
	if fcnt <= 0 {
		db.buildLenIndex()
		return nil
	}

	start := time.Now()

	type (
		rawdata struct {
			k   []byte
			v   []byte
			idx uint64
		}
		interIndex map[string]map[string]*roaring64.Bitmap // field -> value -> ids
	)

	global := make(interIndex, fcnt)
	for name := range db.fields {
		if _, skip := skipFields[name]; skip {
			continue
		}
		global[name] = make(map[string]*roaring64.Bitmap, initialIndexLen)
	}

	jobs := make(chan rawdata, 10000)

	workers := runtime.NumCPU()
	locals := make([]interIndex, workers)
	errors := make([]error, workers)

	localUniverse := make([]*roaring64.Bitmap, workers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(widx int) {
			defer wg.Done()

			local := make(interIndex)
			locals[widx] = local

			for name := range db.fields {
				if _, skip := skipFields[name]; skip {
					continue
				}
				local[name] = make(map[string]*roaring64.Bitmap, 1024)
			}

			lu := roaring64.NewBitmap()
			localUniverse[widx] = lu

			for {
				select {
				case <-ctx.Done():
					return
				case kv, ok := <-jobs:
					if !ok {
						return
					}
					val, err := db.decode(kv.v)
					if err != nil {
						errors[widx] = err
						cancel()
						return
					}
					ptr := unsafe.Pointer(val)
					idx := kv.idx

					lu.Add(idx)

					for name, f := range db.fields {
						if _, skip := skipFields[name]; skip {
							continue
						}
						single, multi, ok := db.getters[name](ptr)
						if !ok {
							continue
						}
						if f.Slice {
							for _, s := range multi {
								addTo(local[name], s, idx)
							}
						} else {
							addTo(local[name], single, idx)
						}
					}
				}
			}
		}(i)
	}

	recordCount := uint64(0)

	err := db.bolt.View(func(tx *bbolt.Tx) error {

		defer wg.Wait()
		defer close(jobs)

		b := tx.Bucket(db.bucket)
		if b == nil {
			return nil
		}
		done := ctx.Done()
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			select {
			case <-done:
				return nil
			case jobs <- rawdata{k, v, db.idxFromKeyNoLock(k)}:
				recordCount++
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("scan error: %w", err)
	}
	for _, err = range errors {
		if err != nil {
			return fmt.Errorf("scan error: %w", err)
		}
	}

	for _, local := range locals {
		if local == nil {
			continue
		}
		for fname, fmap := range local {
			gmap := global[fname]
			for val, lbm := range fmap {
				gbm := gmap[val]
				if gbm == nil {
					gmap[val] = lbm
					continue
				}
				gbm.Or(lbm)
			}
		}
	}

	if db.index == nil {
		db.index = make(map[string]*[]index)
	}

	for name, values := range global {
		s := make([]index, 0, len(values))
		for val, bm := range values {
			bm.RunOptimize()
			s = append(s, index{Key: val, IDs: bm})
		}
		slices.SortFunc(s, func(a, b index) int {
			return strings.Compare(a.Key, b.Key)
		})
		db.index[name] = &s
	}

	universe := roaring64.NewBitmap()
	for _, lu := range localUniverse {
		if lu != nil {
			universe.Or(lu)
		}
	}
	db.universe = universe

	db.stats.IndexBuildTime = time.Since(start)
	db.stats.IndexBuildRPS = int(recordCount / max(uint64(time.Since(start).Seconds()), 1))

	db.buildLenIndex()

	return nil
}

func addTo(m map[string]*roaring64.Bitmap, key string, id uint64) {
	bm := m[key]
	if bm == nil {
		bm = roaring64.NewBitmap()
		m[key] = bm
	}
	bm.Add(id)
}

func (db *DB[K, V]) idxFromKeyNoLock(key []byte) uint64 {
	if db.strkey {
		return db.strmap.createIdxNoLock(string(key))
	}
	return binary.BigEndian.Uint64(key)
}

func (db *DB[K, V]) lastIDNoLock() K {
	if db.universe.IsEmpty() {
		var k K
		return k
	}
	if db.strkey {
		db.strmap.RLock()
		v, _ := db.strmap.getStringNoLock(db.universe.Maximum())
		db.strmap.RUnlock()
		return *(*K)(unsafe.Pointer(&v))
	}
	v := db.universe.Maximum()
	return *(*K)(unsafe.Pointer(&v))
}

func (db *DB[K, V]) idFromIdxNoLock(idx uint64) K {
	if db.strkey {
		v := db.strmap.mustGetStringNoLock(idx)
		return *(*K)(unsafe.Pointer(&v))
	}
	return *(*K)(unsafe.Pointer(&idx))
}

func (db *DB[K, V]) buildLenIndex() {

	if db.universe == nil || db.universe.IsEmpty() {
		return
	}

	if db.lenIndex == nil {
		db.lenIndex = make(map[string]*[]index)
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

			nonEmpty.Or(ix.IDs)

			iter := ix.IDs.Iterator()
			for iter.HasNext() {
				idx := iter.Next()
				counts[idx]++
			}
		}

		lenMap := make(map[uint32]*roaring64.Bitmap, len(counts)+1)

		for idx, ln := range counts {
			if ln == 0 {
				continue
			}
			bm, ok := lenMap[ln]
			if !ok {
				bm = roaring64.NewBitmap()
				lenMap[ln] = bm
			}
			bm.Add(idx)
		}

		empty := db.universe.Clone()
		empty.AndNot(nonEmpty)
		if !empty.IsEmpty() {
			lenMap[0] = empty
		}

		result := make([]index, 0, len(lenMap))
		for ln, bm := range lenMap {
			bm.RunOptimize()
			result = append(result, index{
				Key: uint64ByteStr(uint64(ln)), // big-endian
				IDs: bm,
			})
		}

		slices.SortFunc(result, func(a, b index) int {
			return strings.Compare(a.Key, b.Key)
		})

		db.lenIndex[name] = &result
	}
}

/**/

func (db *DB[K, V]) checkUniqueOnWriteMulti(idxs []uint64, oldVals, newVals []*V, modified [][]string) error {

	leaving := make(map[string]map[string]*roaring64.Bitmap)
	seen := make(map[string]map[string]uint64)

	for i, idx := range idxs {
		oldVal := oldVals[i]
		var newVal *V
		if newVals != nil {
			newVal = newVals[i]
		}
		if oldVal == nil || newVal == nil {
			continue
		}

		ptr := unsafe.Pointer(oldVal)

		for _, name := range modified[i] {
			f := db.fields[name]
			if f == nil || !f.Unique {
				continue
			}
			single, _, ok := db.getters[name](ptr)
			if !ok || single == nilValue {
				continue
			}
			m := leaving[name]
			if m == nil {
				m = make(map[string]*roaring64.Bitmap)
				leaving[name] = m
			}
			bm := m[single]
			if bm == nil {
				bm = roaring64.NewBitmap()
				m[single] = bm
			}
			bm.Add(idx)
		}
	}

	if newVals == nil {
		return nil // deletion
	}

	for i, idx := range idxs {

		newVal := newVals[i]
		if newVal == nil {
			continue
		}

		ptr := unsafe.Pointer(newVal)

		for _, name := range modified[i] {
			f := db.fields[name]
			if f == nil || !f.Unique {
				continue
			}

			single, _, ok := db.getters[name](ptr)
			if !ok || single == nilValue {
				continue
			}

			sm := seen[name]
			if sm == nil {
				sm = make(map[string]uint64)
				seen[name] = sm
			}
			if prev, ok := sm[single]; ok && prev != idx {
				return fmt.Errorf("%w: duplicate value for field %v within batch", ErrUniqueViolation, name)
			}
			sm[single] = idx

			slice := db.index[name]
			if slice == nil {
				continue
			}
			bm := findIndex(slice, single)
			if bm == nil || bm.IsEmpty() {
				continue
			}

			var lv *roaring64.Bitmap
			if fm := leaving[name]; fm != nil {
				lv = fm[single]
			}

			if lv == nil || lv.IsEmpty() {
				if bm.GetCardinality() == 1 && bm.Contains(idx) {
					continue
				}
				return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, name)
			}

			iter := bm.Iterator()
			for iter.HasNext() {
				other := iter.Next()
				if other == idx {
					continue
				}
				if lv.Contains(other) {
					continue
				}
				return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, name)
			}
		}
	}
	return nil
}

func (db *DB[K, V]) setIndexOnSuccessMulti(err error, idxs []uint64, oldVals, newVals []*V, modified [][]string) error {
	if err != nil {
		return err
	}
	if db.noIndex.Load() {
		return nil
	}

	additions := make(map[string]map[string]*roaring64.Bitmap) // move outer maps to sync.pool?
	removals := make(map[string]map[string]*roaring64.Bitmap)

	for i, idx := range idxs {

		oldV := oldVals[i]
		var newV *V
		if newVals != nil {
			newV = newVals[i]
		}

		if oldV != nil {
			ptr := unsafe.Pointer(oldV)

			var mods []string
			if modified != nil {
				mods = modified[i]
			} else {
				mods = db.getModifiedIndexedFields(oldV, newV)
			}

			for _, dbname := range mods {
				f := db.fields[dbname]
				single, multi, ok := db.getters[dbname](ptr)
				if !ok {
					continue
				}

				remField := removals[dbname]
				if remField == nil {
					remField = make(map[string]*roaring64.Bitmap)
					removals[dbname] = remField
				}

				if f.Slice {
					for _, sval := range multi {
						bm := remField[sval]
						if bm == nil {
							bm = roaring64.NewBitmap()
							remField[sval] = bm
						}
						bm.Add(idx)
					}

					if lenSlice := db.lenIndex[dbname]; lenSlice != nil {
						oldLen := distinctCount(multi)
						keyLen := uint64ByteStr(uint64(oldLen))
						if bm := findIndex(lenSlice, keyLen); bm != nil {
							bm.Remove(idx)
						}
					}

				} else {
					bm := remField[single]
					if bm == nil {
						bm = roaring64.NewBitmap()
						remField[single] = bm
					}
					bm.Add(idx)
				}
			}
		}

		if newV != nil {
			ptr := unsafe.Pointer(newV)

			var mods []string
			if modified != nil {
				mods = modified[i]
			} else {
				mods = db.getModifiedIndexedFields(oldV, newV)
			}

			for _, dbname := range mods {
				f := db.fields[dbname]
				single, multi, ok := db.getters[dbname](ptr)
				if !ok {
					continue
				}

				addField := additions[dbname]
				if addField == nil {
					addField = make(map[string]*roaring64.Bitmap)
					additions[dbname] = addField
				}

				if f.Slice {
					for _, sval := range multi {
						bm := addField[sval]
						if bm == nil {
							bm = roaring64.NewBitmap()
							addField[sval] = bm
						}
						bm.Add(idx)
					}

					lenSlice := db.lenIndex[dbname]
					if lenSlice == nil {
						s := make([]index, 0, 8)
						lenSlice = &s
						db.lenIndex[dbname] = lenSlice
					}
					newLen := distinctCount(multi)
					keyLen := uint64ByteStr(uint64(newLen))
					findInsert(lenSlice, keyLen).Add(idx)

				} else {
					bm := addField[single]
					if bm == nil {
						bm = roaring64.NewBitmap()
						addField[single] = bm
					}
					bm.Add(idx)
				}
			}
		}

		switch {
		case oldV == nil && newV != nil:
			db.universe.Add(idx)
		case oldV != nil && newV == nil:
			db.universe.Remove(idx)
		}
	}

	for dbname, removedKeys := range removals {
		slice := db.index[dbname]
		if slice == nil {
			s := make([]index, 0, initialIndexLen)
			slice = &s
			db.index[dbname] = slice
		}
		for key, bmRemoved := range removedKeys {
			if bm := findIndex(slice, key); bm != nil {
				bm.AndNot(bmRemoved)
			}
		}
	}

	for dbname, addedKeys := range additions {
		slice := db.index[dbname]
		if slice == nil {
			s := make([]index, 0, initialIndexLen)
			slice = &s
			db.index[dbname] = slice
		}

		newKeys := make([]index, 0, len(addedKeys))

		for key, bmAdded := range addedKeys {
			if bm := findIndex(slice, key); bm != nil {
				bm.Or(bmAdded)
			} else {
				newKeys = append(newKeys, index{
					Key: key,
					IDs: bmAdded,
				})
			}
		}

		if len(newKeys) == 0 {
			continue
		}

		slices.SortFunc(newKeys, func(a, b index) int {
			return strings.Compare(a.Key, b.Key)
		})

		old := *slice
		merged := make([]index, 0, len(old)+len(newKeys))
		i, k := 0, 0

		for i < len(old) && k < len(newKeys) {
			if old[i].Key < newKeys[k].Key {
				merged = append(merged, old[i])
				i++
			} else if old[i].Key > newKeys[k].Key {
				merged = append(merged, newKeys[k])
				k++
			} else {
				old[i].IDs.Or(newKeys[k].IDs)
				merged = append(merged, old[i])
				i++
				k++
			}
		}

		merged = append(merged, old[i:]...)
		merged = append(merged, newKeys[k:]...)

		*slice = merged
	}

	return nil
}

func distinctCount(s []string) int {
	n := len(s)
	switch n {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		if s[0] != s[1] {
			return 2
		}
		return 1
	case 3:
		a, b, c := s[0], s[1], s[2]
		if a == b {
			if b == c {
				return 1
			}
			return 2
		}
		if a == c || b == c {
			return 2
		}
		return 3
	}
	if n <= 8 {
		return distinctCountLoop(s, n)
	}
	return len(dedupStringsInplace(s))
	// return distinctCountMap(s, n)
}

func distinctCountLoop(s []string, n int) int {
	uniq := 0
OUTER:
	for i := 0; i < n; i++ {
		v := s[i]
		for k := 0; k < i; k++ {
			if s[k] == v {
				continue OUTER
			}
		}
		uniq++
	}
	return uniq
}

func distinctCountMap(s []string, n int) int {
	m := make(map[string]struct{}, n)
	for _, v := range s {
		m[v] = struct{}{}
	}
	return len(m)
}

func (db *DB[K, V]) checkUniqueOnWrite(idx uint64, newVal *V, modified []string) error {
	if newVal == nil {
		return nil
	}

	ptr := unsafe.Pointer(newVal)

	for _, name := range modified {
		f := db.fields[name]
		if f == nil || !f.Unique || f.Slice {
			continue
		}

		single, _, ok := db.getters[name](ptr)
		if !ok {
			continue
		}

		if single == nilValue {
			continue // as in classic sql, multiple nulls are ok
		}

		slice := db.index[name]
		if slice == nil {
			continue
		}
		bm := findIndex(slice, single)
		if bm == nil || bm.IsEmpty() {
			continue
		}
		if bm.GetCardinality() == 1 && bm.Contains(idx) {
			continue
		}
		// value stored in `single` can contain binary - do not print
		return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, name)
	}
	return nil
}

func (db *DB[K, V]) setIndexOnSuccess(err error, idx uint64, oldVal, newVal *V, modified []string) error {
	if err != nil {
		return err
	}
	if db.noIndex.Load() {
		return nil
	}

	if oldVal != nil {
		ptr := unsafe.Pointer(oldVal)
		for _, dbname := range modified {
			f := db.fields[dbname]
			single, multi, ok := db.getters[dbname](ptr)
			if !ok {
				continue
			}
			if slice := db.index[dbname]; slice != nil {
				if f.Slice {
					for _, sval := range multi {
						if bm := findIndex(slice, sval); bm != nil {
							bm.Remove(idx) // removal from the slice will be handled on index save
						}
					}
				} else {
					if bm := findIndex(slice, single); bm != nil {
						bm.Remove(idx) // removal from the slice will be handled on index save
					}
				}
			}
			if f.Slice {
				if lenSlice := db.lenIndex[dbname]; lenSlice != nil {
					oldLen := distinctCount(multi)
					keyLen := uint64ByteStr(uint64(oldLen))
					if bm := findIndex(lenSlice, keyLen); bm != nil {
						bm.Remove(idx)
					}
				}
			}
		}
	}

	if newVal != nil {
		ptr := unsafe.Pointer(newVal)
		for _, dbname := range modified {
			f := db.fields[dbname]
			single, multi, ok := db.getters[dbname](ptr)
			if !ok {
				continue
			}
			slice := db.index[dbname]
			if slice == nil {
				s := make([]index, 0, initialIndexLen)
				slice = &s
				db.index[dbname] = slice
			}
			if f.Slice {
				for _, sval := range multi {
					findInsert(slice, sval).Add(idx)
				}

				lenSlice := db.lenIndex[dbname]
				if lenSlice == nil {
					s := make([]index, 0, 8)
					lenSlice = &s
					db.lenIndex[dbname] = lenSlice
				}
				newLen := distinctCount(multi)
				keyLen := uint64ByteStr(uint64(newLen))
				findInsert(lenSlice, keyLen).Add(idx)

			} else {
				findInsert(slice, single).Add(idx)
			}
		}
	}

	switch {
	case oldVal == nil && newVal != nil:
		db.universe.Add(idx)
	case oldVal != nil && newVal == nil:
		db.universe.Remove(idx)
	}

	return nil
}

func findIndex(a *[]index, key string) *roaring64.Bitmap {
	slice := *a
	lo, hi := 0, len(slice)
	for lo < hi {
		mid := (lo + hi) >> 1
		if slice[mid].Key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(slice) && slice[lo].Key == key {
		return slice[lo].IDs
	}
	return nil
}

func findInsert(a *[]index, key string) *roaring64.Bitmap {
	slice := *a
	lo, hi := 0, len(slice)
	for lo < hi {
		mid := (lo + hi) >> 1
		if slice[mid].Key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(slice) && slice[lo].Key == key {
		return slice[lo].IDs
	}
	slice = append(slice, index{})
	copy(slice[lo+1:], slice[lo:])
	newVal := index{
		Key: key,
		IDs: roaring64.NewBitmap(),
	}
	slice[lo] = newVal
	*a = slice
	return newVal.IDs
}
