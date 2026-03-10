package rbi

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

// MakePatch builds and returns a patch describing fields that changed between
// oldVal and newVal.
//
// The patch includes both indexed and non-indexed fields. For every modified
// field it adds a Field entry whose Name is the Go struct field name,
// and whose Value is a deep copy of the value taken from newVal.
//
// If newVal is nil, it returns an empty slice.
func (db *DB[K, V]) MakePatch(oldVal, newVal *V) []Field {
	return db.makePatch(oldVal, newVal, nil)
}

// MakePatchInto is like MakePatch, but writes the result into the provided
// buffer to reduce allocations.
//
// dst is treated as scratch space: it will be reset to length 0 and then filled
// with the resulting patch. The returned slice may refer to the same underlying
// array or a grown one if capacity is insufficient.
//
// If newVal is nil, it returns an empty slice.
func (db *DB[K, V]) MakePatchInto(oldVal, newVal *V, dst []Field) []Field {
	return db.makePatch(oldVal, newVal, dst)
}

var patchMapPool = sync.Pool{
	New: func() any {
		return make(map[string]struct{})
	},
}

func (db *DB[K, V]) makePatch(oldVal, newVal *V, target []Field) []Field {

	target = target[:0]

	if newVal == nil {
		return target
	}

	var rvOld, rvNew reflect.Value
	if oldVal != nil {
		rvOld = reflect.ValueOf(oldVal).Elem()
	}
	rvNew = reflect.ValueOf(newVal).Elem()

	processed := patchMapPool.Get().(map[string]struct{})
	clear(processed)
	defer patchMapPool.Put(processed)

	mods := db.getModifiedIndexedFields(oldVal, newVal)

	for _, dbname := range mods {
		f := db.fields[dbname]

		processed[f.Name] = struct{}{}

		var newValue any
		if rvNew.IsValid() {
			newValue = rvNew.FieldByIndex(f.Index).Interface()
		}

		target = append(target, Field{
			Name:  f.Name,
			Value: deepCopyValue(newValue),
		})
	}

	for patchKey, f := range db.patchMap {
		if patchKey != f.Name {
			continue
		}
		if _, ok := processed[f.Name]; ok {
			continue
		}

		var v1, v2 any
		if rvOld.IsValid() {
			v1 = rvOld.FieldByIndex(f.Index).Interface()
		}
		if rvNew.IsValid() {
			v2 = rvNew.FieldByIndex(f.Index).Interface()
		}

		if !reflect.DeepEqual(v1, v2) {
			target = append(target, Field{
				Name:  f.Name,
				Value: deepCopyValue(v2),
			})
		}
		processed[f.Name] = struct{}{}
	}

	return target
}

func (db *DB[K, V]) keyFromID(id K) []byte {
	if db.strkey {
		s := *(*string)(unsafe.Pointer(&id))
		return unsafe.Slice(unsafe.StringData(s), len(s))
	}
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], *(*uint64)(unsafe.Pointer(&id)))
	return key[:]
}

func (db *DB[K, V]) idFromKey(b []byte) K {
	if db.strkey {
		s := string(b) // must allocate here because bytes are from bbolt
		return *(*K)(unsafe.Pointer(&s))
	}
	v := binary.BigEndian.Uint64(b)
	return *(*K)(unsafe.Pointer(&v))
}

func (db *DB[K, V]) idxFromID(id K) uint64 {
	idx, _ := db.idxFromIDWithCreated(id)
	return idx
}

func (db *DB[K, V]) idxFromIDWithCreated(id K) (uint64, bool) {
	if db.strkey {
		s := *(*string)(unsafe.Pointer(&id))
		db.strmap.Lock()
		defer db.strmap.Unlock()
		if idx, ok := db.strmap.Keys[s]; ok {
			return idx, false
		}
		idx := db.strmap.createIdxNoLock(s)
		return idx, true
	}
	return *(*uint64)(unsafe.Pointer(&id)), false
}

func (db *DB[K, V]) idxsFromIDWithCreated(ids []K) ([]uint64, []bool) {
	if db.strkey {
		s := *(*[]string)(unsafe.Pointer(&ids))
		idxs := make([]uint64, len(s))
		created := make([]bool, len(s))
		db.strmap.Lock()
		for i := range s {
			if idx, ok := db.strmap.Keys[s[i]]; ok {
				idxs[i] = idx
				continue
			}
			idxs[i] = db.strmap.createIdxNoLock(s[i])
			created[i] = true
		}
		db.strmap.Unlock()
		return idxs, created
	}
	return *(*[]uint64)(unsafe.Pointer(&ids)), nil
}

func (db *DB[K, V]) rollbackCreatedStrIdx(id K, idx uint64) {
	if !db.strkey || idx == 0 {
		return
	}
	s := *(*string)(unsafe.Pointer(&id))

	db.strmap.Lock()
	defer db.strmap.Unlock()

	cur, ok := db.strmap.Keys[s]
	if !ok || cur != idx {
		return
	}

	delete(db.strmap.Keys, s)
	if idx <= uint64(^uint(0)>>1) {
		i := int(idx)
		if i < len(db.strmap.Strs) {
			db.strmap.Strs[i] = ""
		}
		if i < len(db.strmap.strsUsed) {
			db.strmap.strsUsed[i] = false
		}
	}

	if idx <= db.strmap.Next {
		for db.strmap.Next > 0 {
			if db.strmap.Next > uint64(^uint(0)>>1) {
				db.strmap.Next = 0
				break
			}
			i := int(db.strmap.Next)
			if i < len(db.strmap.strsUsed) && db.strmap.strsUsed[i] {
				break
			}
			db.strmap.Next--
		}

		trim := int(db.strmap.Next) + 1
		if trim < len(db.strmap.Strs) {
			clear(db.strmap.Strs[trim:])
			db.strmap.Strs = db.strmap.Strs[:trim]
		}
		if trim < len(db.strmap.strsUsed) {
			clear(db.strmap.strsUsed[trim:])
			db.strmap.strsUsed = db.strmap.strsUsed[:trim]
		}
	}

	if db.strmap.deltaKeys != nil {
		delete(db.strmap.deltaKeys, s)
		if len(db.strmap.deltaKeys) == 0 {
			db.strmap.deltaKeys = nil
		}
	}
	if db.strmap.deltaStrs != nil {
		delete(db.strmap.deltaStrs, idx)
		if len(db.strmap.deltaStrs) == 0 {
			db.strmap.deltaStrs = nil
		}
	}
}

func (db *DB[K, V]) forEachIdxFromID(ids []K, fn func(uint64)) {
	if len(ids) == 0 || fn == nil {
		return
	}

	if db.strkey {
		s := *(*[]string)(unsafe.Pointer(&ids))
		db.strmap.Lock()
		for i := range s {
			fn(db.strmap.createIdxNoLock(s[i]))
		}
		db.strmap.Unlock()
		return
	}

	for _, idx := range *(*[]uint64)(unsafe.Pointer(&ids)) {
		fn(idx)
	}
}

var msgpackEncPool = sync.Pool{New: func() any { return msgpack.NewEncoder(io.Discard) }}
var msgpackDecPool = sync.Pool{New: func() any { return msgpack.NewDecoder(strings.NewReader("")) }}

func (db *DB[K, V]) decode(b []byte) (*V, error) {
	v := db.recPool.Get().(*V)
	dec := msgpackDecPool.Get().(*msgpack.Decoder)
	dec.Reset(bytes.NewReader(b))
	err := dec.Decode(v)
	msgpackDecPool.Put(dec)
	if err != nil {
		db.ReleaseRecords(v)
		return nil, err
	}
	return v, nil
}

func (db *DB[K, V]) encode(v *V, b *bytes.Buffer) error {
	enc := msgpackEncPool.Get().(*msgpack.Encoder)
	enc.Reset(b)
	err := enc.Encode(v)
	msgpackEncPool.Put(enc)
	return err
}

var encodePool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func getEncodeBuf() *bytes.Buffer {
	return encodePool.Get().(*bytes.Buffer)
}

func releaseEncodeBuf(b *bytes.Buffer) {
	b.Reset()
	encodePool.Put(b)
}

func rollback(tx *bbolt.Tx) { _ = tx.Rollback() }

func closeFile(f *os.File) { _ = f.Close() }

func sanitizeSuffix(s string) string {
	out := make([]rune, 0, len(s))
	dot := false

	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			out = append(out, r)
			dot = false
		case r >= 'A' && r <= 'Z':
			out = append(out, r)
			dot = false
		case r >= '0' && r <= '9':
			out = append(out, r)
			dot = false
		case r == '_':
			out = append(out, r)
			dot = false
		case r == '.':
			if !dot {
				out = append(out, r)
			}
			dot = true
		default:
		}
	}
	return strings.Trim(string(out), ".")
}

func dedupStringsInplace(s []string) []string {
	if len(s) < 2 {
		return s
	}
	slices.Sort(s)
	w := 1
	for i := 1; i < len(s); i++ {
		if s[i] != s[w-1] {
			s[w] = s[i]
			w++
		}
	}
	return s[:w]
}

func uint64Bytes(v uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], v)
	return key[:]
}

func uint64ByteStr(v uint64) string {
	b := uint64Bytes(v)
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func int64ByteStr(v int64) string {
	return uint64ByteStr(uint64(v) ^ (uint64(1) << 63))
}

func float64ByteStr(f float64) string {
	u := math.Float64bits(f)
	const sign = uint64(1) << 63
	if u&sign != 0 {
		u = ^u // negative: flip all bits
	} else {
		u ^= sign // non-negative (includes +0): flip sign bit
	}
	return uint64ByteStr(u)
}

var (
	registryMu sync.Mutex
	registry   = make(map[string]struct{})
)

func regInstance(dbPath, bucket string) error {
	registryMu.Lock()
	defer registryMu.Unlock()

	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return fmt.Errorf("error getting absolute file path: %w", err)
	}

	key := abs + "::" + bucket
	if _, exists := registry[key]; exists {
		return fmt.Errorf("rbi is already open for \"%v\" at %v", bucket, dbPath)
	}

	registry[key] = struct{}{}
	return nil
}

func unregInstance(dbPath, bucket string) {
	registryMu.Lock()
	defer registryMu.Unlock()

	absPath, _ := filepath.Abs(dbPath)
	key := absPath + "::" + bucket
	delete(registry, key)
}

const randStreamMix uint64 = 0x9e3779b97f4a7c15

func newRand(seed int64) *rand.Rand {
	s := uint64(seed)
	return rand.New(rand.NewPCG(s, s^randStreamMix))
}
