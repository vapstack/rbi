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
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

func forceMemoryCleanup(releaseOSMemory bool) {
	if releaseOSMemory {
		debug.FreeOSMemory()
		return
	}
	runtime.GC()
}

// PatchOption controls MakePatch behaviour.
type PatchOption uint8

const (
	// PatchJSON makes MakePatch emit json tag names when present.
	// Fields without a json tag fall back to their Go struct field name.
	PatchJSON PatchOption = 1 << iota
)

// MakePatch builds and returns a patch describing fields that changed between
// oldVal and newVal.
//
// The patch includes both indexed and non-indexed fields. For every modified
// field it adds a Field entry whose Name uses the db tag when present or
// Go struct field name otherwise.
//
// When PatchJSON is passed, Name uses the json tag when present or
// Go struct field name otherwise.
//
// Value is always a deep copy taken from newVal.
//
// If newVal is nil, it returns an empty slice.
func (db *DB[K, V]) MakePatch(oldVal, newVal *V, opts ...PatchOption) []Field {
	useJSON := false
	for _, opt := range opts {
		if opt == PatchJSON {
			useJSON = true
		}
	}
	return db.makePatch(oldVal, newVal, nil, useJSON)
}

// MakePatchInto is like MakePatch, but writes the result into the provided
// buffer to reduce allocations.
//
// dst is treated as scratch space: it will be reset to length 0 and then filled
// with the resulting patch. The returned slice may refer to the same underlying
// array or a grown one if capacity is insufficient.
//
// If newVal is nil, it returns an empty slice.
func (db *DB[K, V]) MakePatchInto(oldVal, newVal *V, dst []Field, opts ...PatchOption) []Field {
	useJSON := false
	for _, opt := range opts {
		if opt == PatchJSON {
			useJSON = true
		}
	}
	return db.makePatch(oldVal, newVal, dst, useJSON)
}

type patchScratch struct {
	seen []bool
}

var patchScratchPool = pooled.Pointers[patchScratch]{
	Cleanup: func(scratch *patchScratch) {
		clear(scratch.seen[:cap(scratch.seen)])
		scratch.seen = scratch.seen[:0]
	},
}

func (db *DB[K, V]) makePatch(oldVal, newVal *V, target []Field, useJSON bool) []Field {
	target = target[:0]

	if newVal == nil {
		return target
	}

	var rvOld, rvNew reflect.Value
	if oldVal != nil {
		rvOld = reflect.ValueOf(oldVal).Elem()
	}
	rvNew = reflect.ValueOf(newVal).Elem()

	scratch := patchScratchPool.Get()
	scratch.seen = slices.Grow(scratch.seen[:0], len(db.patchFieldAccess))[:len(db.patchFieldAccess)]
	defer patchScratchPool.Put(scratch)

	newPtr := unsafe.Pointer(newVal)
	oldPtr := unsafe.Pointer(nil)
	if oldVal != nil {
		oldPtr = unsafe.Pointer(oldVal)
	}

	db.forEachModifiedIndexedField(oldVal, newVal, func(acc indexedFieldAccessor) bool {
		if acc.patchOrdinal < 0 {
			return true
		}
		patchAcc := db.patchFieldAccess[acc.patchOrdinal]
		var value any
		if patchAcc.copyValue != nil {
			value = patchAcc.copyValue(newPtr)
		} else {
			value = deepCopyValue(rvNew.FieldByIndex(patchAcc.field.Index).Interface())
		}
		name := patchAcc.field.DBName
		if useJSON {
			name = patchAcc.field.JSONName
		}
		scratch.seen[acc.patchOrdinal] = true
		target = append(target, Field{
			Name:  name,
			Value: value,
		})
		return true
	})

	for ordinal, patchAcc := range db.patchFieldAccess {
		if scratch.seen[ordinal] {
			continue
		}

		var newValue any
		if rvOld.IsValid() {
			if patchAcc.valueEqual != nil {
				if patchAcc.valueEqual(oldPtr, newPtr) {
					continue
				}
			} else {
				oldValue := rvOld.FieldByIndex(patchAcc.field.Index).Interface()
				newValue = rvNew.FieldByIndex(patchAcc.field.Index).Interface()
				if reflect.DeepEqual(oldValue, newValue) {
					continue
				}
			}
		}
		if patchAcc.copyValue != nil {
			newValue = patchAcc.copyValue(newPtr)
		} else if newValue == nil {
			newValue = deepCopyValue(rvNew.FieldByIndex(patchAcc.field.Index).Interface())
		} else {
			newValue = deepCopyValue(newValue)
		}
		name := patchAcc.field.DBName
		if useJSON {
			name = patchAcc.field.JSONName
		}

		target = append(target, Field{
			Name:  name,
			Value: newValue,
		})
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
	if db.strmap.sparseStrs != nil {
		delete(db.strmap.sparseStrs, idx)
	} else if idx <= uint64(^uint(0)>>1) {
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
			if db.strmap.sparseStrs != nil {
				if _, ok := db.strmap.sparseStrs[db.strmap.Next]; ok {
					break
				}
				db.strmap.Next--
				continue
			}
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

		if db.strmap.sparseStrs == nil {
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
	}

	// Failed pre-commit staging may already have advanced internal snapshot
	// caches past the committed state. Restore the committed writer-side base
	// so the next successful publish can stay on the cheap append-only delta
	// path instead of rematerializing the whole mapper.
	db.strmap.restoreCommittedNoLock()
}

func (db *DB[K, V]) addCheckedIdxsFromIDs(ids []K, dst *postingLazySetBuilder) uint64 {
	if len(ids) == 0 || dst == nil {
		return 0
	}

	dedupe := uint64(0)
	if db.strkey {
		s := *(*[]string)(unsafe.Pointer(&ids))
		db.strmap.Lock()
		for i := range s {
			if !dst.addChecked(db.strmap.createIdxNoLock(s[i])) {
				dedupe++
			}
		}
		db.strmap.Unlock()
		return dedupe
	}

	for _, idx := range *(*[]uint64)(unsafe.Pointer(&ids)) {
		if !dst.addChecked(idx) {
			dedupe++
		}
	}
	return dedupe
}

var msgpackEncPool = pooled.Pointers[msgpack.Encoder]{
	New: func() *msgpack.Encoder { return msgpack.NewEncoder(io.Discard) },
	Cleanup: func(enc *msgpack.Encoder) {
		enc.Reset(io.Discard)
	},
}

var msgpackDecPool = pooled.Pointers[msgpack.Decoder]{
	New: func() *msgpack.Decoder { return msgpack.NewDecoder(strings.NewReader("")) },
}

var msgpackReaderPool = pooled.Pointers[bytes.Reader]{Clear: true}

func (db *DB[K, V]) decode(b []byte) (*V, error) {
	v := db.recPool.Get()
	dec := msgpackDecPool.Get()
	reader := msgpackReaderPool.Get()
	reader.Reset(b)
	dec.Reset(reader)
	err := dec.Decode(v)
	msgpackReaderPool.Put(reader)
	msgpackDecPool.Put(dec)
	if err != nil {
		db.ReleaseRecords(v)
		return nil, err
	}
	return v, nil
}

func (db *DB[K, V]) encode(v *V, b *bytes.Buffer) error {
	enc := msgpackEncPool.Get()
	enc.Reset(b)
	err := enc.Encode(v)
	msgpackEncPool.Put(enc)
	return err
}

var encodePool pooled.Buffers

func rollback(tx *bbolt.Tx) { _ = tx.Rollback() }

func closeFile(f *os.File) { _ = f.Close() }

func syncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		dir = "."
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer closeFile(f)
	return f.Sync()
}

func validateBucketName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty", ErrInvalidBucketName)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if i == 0 {
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
				continue
			}
			return fmt.Errorf(
				"%w %q: allowed pattern is [A-Za-z_][A-Za-z0-9_]*",
				ErrInvalidBucketName,
				name,
			)
		}
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			continue
		}
		return fmt.Errorf(
			"%w %q: allowed pattern is [A-Za-z_][A-Za-z0-9_]*",
			ErrInvalidBucketName,
			name,
		)
	}
	return nil
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

func orderedInt64Key(v int64) uint64 {
	return uint64(v) ^ (uint64(1) << 63)
}

func int64ByteStr(v int64) string {
	return uint64ByteStr(orderedInt64Key(v))
}

const canonicalFloat64NaNBits uint64 = 0x7ff8000000000001

func canonicalizeFloat64ForIndex(f float64) float64 {
	switch {
	case math.IsNaN(f):
		return math.Float64frombits(canonicalFloat64NaNBits)
	case f == 0:
		return 0
	default:
		return f
	}
}

func orderedFloat64Key(f float64) uint64 {
	u := math.Float64bits(canonicalizeFloat64ForIndex(f))
	const sign = uint64(1) << 63
	if u&sign != 0 {
		return ^u
	}
	return u ^ sign
}

func compareFloat64QuerySemantics(a, b float64) int {
	a = canonicalizeFloat64ForIndex(a)
	b = canonicalizeFloat64ForIndex(b)

	aNaN := math.IsNaN(a)
	bNaN := math.IsNaN(b)
	switch {
	case aNaN && bNaN:
		return 0
	case aNaN:
		return 1
	case bNaN:
		return -1
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func float64ByteStr(f float64) string {
	return uint64ByteStr(orderedFloat64Key(f))
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
