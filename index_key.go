package rbi

import (
	"strings"
	"unsafe"
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

func indexKeyFromString(s string) indexKey {
	if len(s) == 0 {
		return indexKey{}
	}
	return indexKey{
		ptr:  unsafe.StringData(s),
		meta: uint64(len(s)),
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

func (k indexKey) asString() string {
	if k.isNumeric() {
		return uint64ByteStr(k.meta)
	}
	if k.ptr == nil {
		return ""
	}
	return unsafe.String(k.ptr, k.byteLen())
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
