package keycodec

import (
	"encoding/binary"
	"math"
	"math/bits"
	"strconv"
	"strings"
	"sync"
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

func FromFixed8String(s string) IndexKey {
	if len(s) != 8 {
		return FromString(s)
	}
	return FromU64(Fixed8StringToU64(s))
}

func FromStoredString(s string, fixed8 bool) IndexKey {
	if fixed8 && len(s) == 8 {
		return FromFixed8String(s)
	}
	return FromString(s)
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

func (k IndexKey) ByteAt(i int) byte {
	if k.IsNumeric() {
		shift := uint(56 - i*8)
		return byte(k.meta >> shift)
	}
	return *(*byte)(unsafe.Add(unsafe.Pointer(k.ptr), i))
}

func Fixed8StringToU64(s string) uint64 {
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
	if len(s) == 8 {
		u := Fixed8StringToU64(s)
		if v < u {
			return -1
		}
		if v > u {
			return 1
		}
		return 0
	}
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
	return -1
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
	return len(b) == 8 && a.meta == Fixed8StringToU64(b)
}

func HasPrefixString(a IndexKey, prefix string) bool {
	if a.ptr != indexKeyNumericSentinel {
		return strings.HasPrefix(unsafe.String(a.ptr, int(a.meta)), prefix)
	}
	switch len(prefix) {
	case 0:
		return true
	case 1:
		return byte(a.meta>>56) == prefix[0]
	case 2:
		return uint16(a.meta>>48) == uint16(prefix[0])<<8|uint16(prefix[1])
	case 3:
		return uint32(a.meta>>40) == uint32(prefix[0])<<16|uint32(prefix[1])<<8|uint32(prefix[2])
	case 4:
		return uint32(a.meta>>32) == uint32(prefix[0])<<24|uint32(prefix[1])<<16|uint32(prefix[2])<<8|uint32(prefix[3])
	case 5:
		return a.meta>>24 == uint64(prefix[0])<<32|uint64(prefix[1])<<24|uint64(prefix[2])<<16|uint64(prefix[3])<<8|uint64(prefix[4])
	case 6:
		return a.meta>>16 == uint64(prefix[0])<<40|uint64(prefix[1])<<32|uint64(prefix[2])<<24|uint64(prefix[3])<<16|uint64(prefix[4])<<8|uint64(prefix[5])
	case 7:
		return a.meta>>8 == uint64(prefix[0])<<48|uint64(prefix[1])<<40|uint64(prefix[2])<<32|uint64(prefix[3])<<24|uint64(prefix[4])<<16|uint64(prefix[5])<<8|uint64(prefix[6])
	case 8:
		return a.meta == Fixed8StringToU64(prefix)
	default:
		return false
	}
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
	if 8 > upper.cutLen {
		return 1
	}
	return 0
}

func HasSuffixString(a IndexKey, suffix string) bool {
	if a.ptr != indexKeyNumericSentinel {
		return strings.HasSuffix(unsafe.String(a.ptr, int(a.meta)), suffix)
	}
	switch len(suffix) {
	case 0:
		return true
	case 1:
		return byte(a.meta) == suffix[0]
	case 2:
		return uint16(a.meta) == uint16(suffix[0])<<8|uint16(suffix[1])
	case 3:
		return uint32(a.meta&0xffffff) == uint32(suffix[0])<<16|uint32(suffix[1])<<8|uint32(suffix[2])
	case 4:
		return uint32(a.meta) == uint32(suffix[0])<<24|uint32(suffix[1])<<16|uint32(suffix[2])<<8|uint32(suffix[3])
	case 5:
		return a.meta&0xffffffffff == uint64(suffix[0])<<32|uint64(suffix[1])<<24|uint64(suffix[2])<<16|uint64(suffix[3])<<8|uint64(suffix[4])
	case 6:
		return a.meta&0xffffffffffff == uint64(suffix[0])<<40|uint64(suffix[1])<<32|uint64(suffix[2])<<24|uint64(suffix[3])<<16|uint64(suffix[4])<<8|uint64(suffix[5])
	case 7:
		return a.meta&0xffffffffffffff == uint64(suffix[0])<<48|uint64(suffix[1])<<40|uint64(suffix[2])<<32|uint64(suffix[3])<<24|uint64(suffix[4])<<16|uint64(suffix[5])<<8|uint64(suffix[6])
	case 8:
		return a.meta == Fixed8StringToU64(suffix)
	default:
		return false
	}
}

func ContainsString(a IndexKey, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if a.ptr != indexKeyNumericSentinel {
		return strings.Contains(unsafe.String(a.ptr, int(a.meta)), needle)
	}
	return containsU64String(a.meta, needle)
}

func containsU64String(v uint64, needle string) bool {
	alen := 8
	nlen := len(needle)
	if nlen > alen {
		return false
	}
	limit := alen - nlen
	for i := 0; i <= limit; i++ {
		if byte(v>>uint(56-i*8)) != needle[0] {
			continue
		}
		ok := true
		for j := 1; j < nlen; j++ {
			if byte(v>>uint(56-(i+j)*8)) != needle[j] {
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

func U64Bytes(v uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], v)
	return key[:]
}

func U64ByteString(v uint64) string {
	b := U64Bytes(v)
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func OrderedInt64Key(v int64) uint64 {
	return uint64(v) ^ (uint64(1) << 63)
}

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

func Float64ByteString(f float64) string {
	return U64ByteString(OrderedFloat64Key(f))
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

func UserKeyFromDataKey[K ~string | ~uint64](key DataKey, strKey bool) K {
	if strKey {
		s := key.s
		return *(*K)(unsafe.Pointer(&s))
	}
	u := key.u
	return *(*K)(unsafe.Pointer(&u))
}

func UserKeyBytesWithBuf[K ~string | ~uint64](id K, strKey bool, buf *[8]byte) []byte {
	if strKey {
		s := *(*string)(unsafe.Pointer(&id))
		return unsafe.Slice(unsafe.StringData(s), len(s))
	}
	binary.BigEndian.PutUint64(buf[:], *(*uint64)(unsafe.Pointer(&id)))
	return buf[:]
}

func UserKeyFromBytes[K ~string | ~uint64](b []byte, strKey bool) K {
	if strKey {
		s := string(b)
		return *(*K)(unsafe.Pointer(&s))
	}
	v := binary.BigEndian.Uint64(b)
	return *(*K)(unsafe.Pointer(&v))
}

func (key DataKey) Bytes(strKey bool, buf *[8]byte) []byte {
	if strKey {
		return unsafe.Slice(unsafe.StringData(key.s), len(key.s))
	}
	binary.BigEndian.PutUint64(buf[:], key.u)
	return buf[:]
}

func (key DataKey) Format(strKey bool) string {
	if strKey {
		return key.s
	}
	return strconv.FormatUint(key.u, 10)
}

func (key DataKey) String() string {
	return key.s
}

func (key DataKey) Uint() uint64 {
	return key.u
}

/**/

const (
	minDataKeyShift = 5
	maxDataKeyShift = 16

	MinDataKeyPooledCap = 1 << minDataKeyShift
	MaxDataKeyPooledCap = 1 << maxDataKeyShift

	// MaxDataKeyRetainedCap is the largest external capacity that may be
	// demoted into the largest bucket. Larger slices are dropped to avoid
	// retaining very large backing arrays through a smaller bucket.
	MaxDataKeyRetainedCap = MaxDataKeyPooledCap + MaxDataKeyPooledCap/4

	maxBucketDistance = 4
)

var stringPools [maxDataKeyShift + 1]sync.Pool

func GetDataKeySlice(capHint int) []string {
	if capHint < 0 {
		panic("GetStringSlice: negative capHint")
	}

	if capHint > MaxDataKeyPooledCap {
		return make([]string, 0, capHint)
	}
	var shift int
	if capHint <= MinDataKeyPooledCap {
		shift = minDataKeyShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+maxBucketDistance, maxDataKeyShift)
	for sh := shift; sh <= lim; sh++ {
		if v := stringPools[sh].Get(); v != nil {
			ptr := v.(*string)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]string, 0, 1<<shift)
}

func PutDataKeySlice(s []string) {
	c := cap(s)
	if c < MinDataKeyPooledCap {
		return
	}
	var shift int
	if c >= MaxDataKeyPooledCap {
		if c <= MaxDataKeyRetainedCap {
			shift = maxDataKeyShift
		} else {
			return
		}
	} else {
		shift = bits.Len(uint(c)) - 1 // floor(log2(c))
	}
	clear(s[:c])
	n := 1 << shift
	stringPools[shift].Put(unsafe.SliceData(s[:n:n]))
}
