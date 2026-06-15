package keycodec

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"unsafe"
)

var indexKeyNumericSentinel = new(byte)

// IndexKey stores either:
// - numeric fixed-width key as uint64 (ptr == indexKeyNumericSentinel), or
// - string key bytes (ptr points to first byte, meta stores length).
type IndexKey struct {
	ptr  *byte
	meta uint64
}

func FromString(s string) IndexKey {
	if len(s) == 0 {
		return IndexKey{}
	}
	return IndexKey{
		ptr:  unsafe.StringData(s),
		meta: uint64(len(s)),
	}
}

func FromBytes(b []byte) IndexKey {
	if len(b) == 0 {
		return IndexKey{}
	}
	return IndexKey{
		ptr:  unsafe.SliceData(b),
		meta: uint64(len(b)),
	}
}

func FromU64(v uint64) IndexKey {
	return IndexKey{
		ptr:  indexKeyNumericSentinel,
		meta: v,
	}
}

func (k IndexKey) IsNumeric() bool {
	return k.ptr == indexKeyNumericSentinel
}

func (k IndexKey) U64() uint64 {
	return k.meta
}

func (k IndexKey) ByteLen() int {
	if k.IsNumeric() {
		return 8
	}
	return int(k.meta)
}

func (k IndexKey) UnsafeString() string {
	if k.IsNumeric() {
		return U64ByteString(k.meta)
	}
	return unsafe.String(k.ptr, int(k.meta))
}

func (k IndexKey) AppendBytes(dst []byte) []byte {
	if k.IsNumeric() {
		return AppendU64Bytes(dst, k.meta)
	}
	return append(dst, unsafe.Slice(k.ptr, int(k.meta))...)
}

func Compare(a, b IndexKey) int {
	if a.ptr == indexKeyNumericSentinel && b.ptr == indexKeyNumericSentinel {
		if a.meta < b.meta {
			return -1
		}
		if a.meta > b.meta {
			return 1
		}
		return 0
	}
	if a.ptr != indexKeyNumericSentinel && b.ptr != indexKeyNumericSentinel {
		return strings.Compare(unsafe.String(a.ptr, int(a.meta)), unsafe.String(b.ptr, int(b.meta)))
	}
	if a.ptr == indexKeyNumericSentinel {
		return compareU64String(a.meta, unsafe.String(b.ptr, int(b.meta)))
	}
	return -compareU64String(b.meta, unsafe.String(a.ptr, int(a.meta)))
}

func compareU64String(v uint64, s string) int {
	slen := len(s)
	n := min(8, slen)
	for i := 0; i < n; i++ {
		ab := byte(v >> uint(56-i*8))
		bb := s[i]
		if ab < bb {
			return -1
		}
		if ab > bb {
			return 1
		}
	}
	if 8 > slen {
		return 1
	}
	if 8 < slen {
		return -1
	}
	return 1
}

func CompareString(a IndexKey, b string) int {
	if a.ptr != indexKeyNumericSentinel {
		return strings.Compare(unsafe.String(a.ptr, int(a.meta)), b)
	}
	return compareU64String(a.meta, b)
}

func EqualsString(a IndexKey, b string) bool {
	if a.ptr != indexKeyNumericSentinel {
		return unsafe.String(a.ptr, int(a.meta)) == b
	}
	return false
}

func HasPrefixString(a IndexKey, prefix string) bool {
	if a.ptr != indexKeyNumericSentinel {
		return strings.HasPrefix(unsafe.String(a.ptr, int(a.meta)), prefix)
	}
	return len(prefix) == 0
}

type PrefixUpperBound struct {
	prefix   string
	cutLen   int
	lastByte byte
}

func NewPrefixUpperBound(prefix string) (PrefixUpperBound, bool) {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] == 0xFF {
			continue
		}
		return PrefixUpperBound{
			prefix:   prefix,
			cutLen:   i + 1,
			lastByte: prefix[i] + 1,
		}, true
	}
	return PrefixUpperBound{}, false
}

func CompareStringPrefixUpperBound(s string, upper PrefixUpperBound) int {
	cutLen := upper.cutLen
	last := cutLen - 1
	slen := len(s)
	if slen < last {
		if c := strings.Compare(s, upper.prefix[:slen]); c != 0 {
			return c
		}
		return -1
	}
	if last > 0 {
		if c := strings.Compare(s[:last], upper.prefix[:last]); c != 0 {
			return c
		}
	}
	if slen == last {
		return -1
	}
	b := s[last]
	if b < upper.lastByte {
		return -1
	}
	if b > upper.lastByte {
		return 1
	}
	if slen > cutLen {
		return 1
	}
	return 0
}

func ComparePrefixUpperBound(a IndexKey, upper PrefixUpperBound) int {
	if a.ptr != indexKeyNumericSentinel {
		return CompareStringPrefixUpperBound(unsafe.String(a.ptr, int(a.meta)), upper)
	}
	n := min(8, upper.cutLen)
	last := upper.cutLen - 1
	for i := 0; i < n; i++ {
		ub := upper.prefix[i]
		if i == last {
			ub = upper.lastByte
		}
		ab := byte(a.meta >> uint(56-i*8))
		if ab < ub {
			return -1
		}
		if ab > ub {
			return 1
		}
	}
	if 8 < upper.cutLen {
		return -1
	}
	return 1
}

func HasSuffixString(a IndexKey, suffix string) bool {
	if a.ptr != indexKeyNumericSentinel {
		return strings.HasSuffix(unsafe.String(a.ptr, int(a.meta)), suffix)
	}
	return len(suffix) == 0
}

func ContainsString(a IndexKey, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if a.ptr != indexKeyNumericSentinel {
		return strings.Contains(unsafe.String(a.ptr, int(a.meta)), needle)
	}
	return false
}

func AppendU64Bytes(dst []byte, v uint64) []byte {
	return append(dst,
		byte(v>>56),
		byte(v>>48),
		byte(v>>40),
		byte(v>>32),
		byte(v>>24),
		byte(v>>16),
		byte(v>>8),
		byte(v),
	)
}

func U64BytesWithBuf(v uint64, buf *[8]byte) []byte {
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

// U64ByteString is an allocationful compatibility helper. Hot paths should
// pass numeric keys as IndexKey/uint64 or write bytes into caller-owned storage.
func U64ByteString(v uint64) string {
	var key [8]byte
	b := U64BytesWithBuf(v, &key)
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func OrderedInt64Key(v int64) uint64 {
	return uint64(v) ^ (uint64(1) << 63)
}

func Int64FromOrderedKey(key uint64) int64 {
	return int64(key ^ (uint64(1) << 63))
}

// Int64ByteString is an allocationful compatibility helper.
func Int64ByteString(v int64) string {
	return U64ByteString(OrderedInt64Key(v))
}

const (
	CanonicalFloat64NaNBits uint64 = 0x7ff8000000000001
	float64ExponentMask     uint64 = 0x7ff0000000000000
	float64MantissaMask     uint64 = 0x000fffffffffffff
)

func CanonicalizeFloat64ForIndex(f float64) float64 {
	u := math.Float64bits(f)
	if u<<1 == 0 {
		return 0
	}
	if u&float64ExponentMask == float64ExponentMask && u&float64MantissaMask != 0 {
		return math.Float64frombits(CanonicalFloat64NaNBits)
	}
	return f
}

func OrderedFloat64Key(f float64) uint64 {
	u := math.Float64bits(f)
	if u<<1 == 0 {
		u = 0
	} else if u&float64ExponentMask == float64ExponentMask && u&float64MantissaMask != 0 {
		u = CanonicalFloat64NaNBits
	}
	const sign = uint64(1) << 63
	if u&sign != 0 {
		return ^u
	}
	return u ^ sign
}

func Float64FromOrderedKey(key uint64) float64 {
	const sign = uint64(1) << 63
	if key&sign != 0 {
		return math.Float64frombits(key ^ sign)
	}
	return math.Float64frombits(^key)
}

// Float64ByteString is an allocationful compatibility helper.
func Float64ByteString(f float64) string {
	return U64ByteString(OrderedFloat64Key(f))
}

type IndexLookupKey struct {
	s       string
	u       uint64
	numeric bool
}

func IndexLookupString(s string) IndexLookupKey {
	return IndexLookupKey{s: s}
}

func IndexLookupU64(u uint64) IndexLookupKey {
	return IndexLookupKey{u: u, numeric: true}
}

func (key IndexLookupKey) IsNumeric() bool {
	return key.numeric
}

func (key IndexLookupKey) StringKey() string {
	return key.s
}

func (key IndexLookupKey) U64() uint64 {
	return key.u
}

func (key IndexLookupKey) IndexKey() IndexKey {
	if key.numeric {
		return FromU64(key.u)
	}
	return FromString(key.s)
}

type DataKey struct {
	s string
	u uint64
}

func DataKeyFromUserKey[K ~string | ~uint64](id K, strKey bool) DataKey {
	if strKey {
		return DataKey{s: *(*string)(unsafe.Pointer(&id))}
	}
	return DataKey{u: *(*uint64)(unsafe.Pointer(&id))}
}

func DataKeyFromBytes(b []byte, strKey bool) DataKey {
	if strKey {
		return DataKey{s: string(b)}
	}
	return DataKey{u: U64FromBytes(b)}
}

func UserKeyFromDataKey[K ~string | ~uint64](key DataKey, strKey bool) K {
	if strKey {
		s := key.s
		return *(*K)(unsafe.Pointer(&s))
	}
	u := key.u
	return *(*K)(unsafe.Pointer(&u))
}

// UserKeyString returns id as the canonical string key carrier.
// It must only be called on paths already known to use string keys.
func UserKeyString[K ~string | ~uint64](id K) string {
	return *(*string)(unsafe.Pointer(&id))
}

// UserKeyUint returns id as the canonical uint64 key carrier.
// It must only be called on paths already known to use uint64 keys.
func UserKeyUint[K ~string | ~uint64](id K) uint64 {
	return *(*uint64)(unsafe.Pointer(&id))
}

// UserKeyStringScanFunc returns fn reinterpreted as a string-key scan callback.
//
// It converts the function value once instead of returning a wrapper that
// converts every scanned key before calling fn.
//
// ScanKeys invokes the callback for every key, so a wrapper would add an extra
// indirect call per key and put key conversion back into the hot loop.
//
// The caller must only use this adapter on paths already known to use string keys.
func UserKeyStringScanFunc[K ~string | ~uint64](fn func(K) (bool, error)) func(string) (bool, error) {
	return *(*func(string) (bool, error))(unsafe.Pointer(&fn))
}

// UserKeyUintScanFunc returns fn reinterpreted as a uint64-key scan callback.
//
// See UserKeyStringScanFunc for explanation.
//
// The caller must only use this adapter on paths already known to use uint64 keys.
func UserKeyUintScanFunc[K ~string | ~uint64](fn func(K) (bool, error)) func(uint64) (bool, error) {
	return *(*func(uint64) (bool, error))(unsafe.Pointer(&fn))
}

func UserKeyBytesWithBuf[K ~string | ~uint64](id K, strKey bool, buf *[8]byte) []byte {
	if strKey {
		s := *(*string)(unsafe.Pointer(&id))
		return StringBytes(s)
	}
	return U64BytesWithBuf(*(*uint64)(unsafe.Pointer(&id)), buf)
}

func UserKeyFromBytes[K ~string | ~uint64](b []byte, strKey bool) K {
	if strKey {
		s := string(b)
		return *(*K)(unsafe.Pointer(&s))
	}
	v := U64FromBytes(b)
	return *(*K)(unsafe.Pointer(&v))
}

func StringBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func U64FromBytes(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func (key DataKey) Bytes(strKey bool, buf *[8]byte) []byte {
	if strKey {
		return StringBytes(key.s)
	}
	return U64BytesWithBuf(key.u, buf)
}

func (key DataKey) Format(strKey bool) string {
	if strKey {
		return key.s
	}
	return strconv.FormatUint(key.u, 10)
}

func (key DataKey) String() string { return key.s }
func (key DataKey) Uint() uint64   { return key.u }
