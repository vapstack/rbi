package keycodec

import (
	"encoding/binary"
	"math"
	"slices"
	"testing"
)

var (
	benchIndexKeyA = FromString("member:000000000001")
	benchIndexKeyB = FromString("member:000000000099")
	benchDBKey     = DataKeyFromUserKey(uint64(0x0102030405060708), false)
	benchBytes     []byte
	benchString    string
	benchInt       int
	benchBool      bool
	benchUint64    uint64
)

func TestIndexKeyNumericAndRaw8(t *testing.T) {
	raw := "\x01\x02\x03\x04\x05\x06\x07\x08"
	want := binary.BigEndian.Uint64([]byte(raw))
	if got := Fixed8StringToU64(raw); got != want {
		t.Fatalf("Fixed8StringToU64: got=%x want=%x", got, want)
	}

	key := FromFixed8String(raw)
	if !key.IsNumeric() {
		t.Fatalf("raw-8 key must be numeric")
	}
	if key.U64() != want {
		t.Fatalf("numeric value mismatch: got=%x want=%x", key.U64(), want)
	}
	if key.ByteLen() != 8 {
		t.Fatalf("numeric byte len: got=%d want=8", key.ByteLen())
	}
	if key.UnsafeString() != raw {
		t.Fatalf("numeric unsafe string mismatch")
	}
	for i := 0; i < 8; i++ {
		if key.ByteAt(i) != raw[i] {
			t.Fatalf("ByteAt(%d): got=%x want=%x", i, key.ByteAt(i), raw[i])
		}
	}
}

func TestIndexKeyStringComparisons(t *testing.T) {
	alpha := FromString("alpha")
	beta := FromBytes([]byte("beta"))
	if alpha.IsNumeric() || beta.IsNumeric() {
		t.Fatalf("string keys must not be numeric")
	}
	if Compare(alpha, beta) >= 0 || Compare(beta, alpha) <= 0 || Compare(alpha, FromString("alpha")) != 0 {
		t.Fatalf("Compare returned unexpected order")
	}
	if CompareString(alpha, "alpha") != 0 || CompareString(alpha, "alphb") >= 0 {
		t.Fatalf("CompareString returned unexpected order")
	}
	if !EqualsString(alpha, "alpha") || EqualsString(alpha, "beta") {
		t.Fatalf("EqualsString mismatch")
	}
	if !HasPrefixString(alpha, "al") || !HasSuffixString(alpha, "ha") || !ContainsString(alpha, "ph") {
		t.Fatalf("substring predicates failed")
	}
}

func TestPrefixUpperBound(t *testing.T) {
	upper, ok := NewPrefixUpperBound("ab\xff")
	if !ok {
		t.Fatalf("expected upper bound")
	}
	if CompareStringPrefixUpperBound("ab\xff", upper) >= 0 {
		t.Fatalf("prefix value must be below upper bound")
	}
	if CompareStringPrefixUpperBound("ac", upper) != 0 {
		t.Fatalf("upper bound should equal ac")
	}
	if ComparePrefixUpperBound(FromString("ab\xffz"), upper) >= 0 {
		t.Fatalf("index key prefix value must be below upper bound")
	}
	if _, ok := NewPrefixUpperBound("\xff\xff"); ok {
		t.Fatalf("all-ff prefix must not have an exclusive upper bound")
	}
}

func TestNumericKeyStringPredicatesUseRaw8Order(t *testing.T) {
	key := FromU64(0x6162636465666768)
	if CompareString(key, "abcdefgh") != 0 {
		t.Fatalf("numeric raw-8 compare mismatch")
	}
	if !EqualsString(key, "abcdefgh") {
		t.Fatalf("numeric raw-8 equality mismatch")
	}
	if !HasPrefixString(key, "abc") || !HasSuffixString(key, "fgh") || !ContainsString(key, "cde") {
		t.Fatalf("numeric raw-8 substring predicates failed")
	}
}

func TestIndexKeyStoredStringConstructors(t *testing.T) {
	raw := "\x01\x02\x03\x04\x05\x06\x07\x08"
	want := Fixed8StringToU64(raw)

	fixed := FromStoredString(raw, true)
	if !fixed.IsNumeric() || fixed.U64() != want || fixed.UnsafeString() != raw {
		t.Fatalf("fixed stored raw-8 mismatch: numeric=%v u64=%x raw=%q", fixed.IsNumeric(), fixed.U64(), fixed.UnsafeString())
	}

	plain := FromStoredString(raw, false)
	if plain.IsNumeric() || plain.UnsafeString() != raw {
		t.Fatalf("plain stored raw-8 mismatch: numeric=%v raw=%q", plain.IsNumeric(), plain.UnsafeString())
	}

	short := FromStoredString("short", true)
	if short.IsNumeric() || short.UnsafeString() != "short" {
		t.Fatalf("short fixed stored string mismatch: numeric=%v raw=%q", short.IsNumeric(), short.UnsafeString())
	}

	empty := FromStoredString("", true)
	if empty.IsNumeric() || empty.ByteLen() != 0 || empty.UnsafeString() != "" {
		t.Fatalf("empty stored key mismatch: numeric=%v len=%d raw=%q", empty.IsNumeric(), empty.ByteLen(), empty.UnsafeString())
	}
}

func TestIndexKeyMixedComparisonsUseByteOrder(t *testing.T) {
	key := FromU64(0x6162636465666768)
	if Compare(key, FromString("abcdefgh")) != 0 {
		t.Fatalf("mixed raw-8 compare equality mismatch")
	}
	if Compare(key, FromString("abcdefg")) <= 0 {
		t.Fatalf("numeric key should sort after shorter matching string")
	}
	if Compare(key, FromString("abcdefghi")) >= 0 {
		t.Fatalf("numeric key should sort before longer matching string")
	}
	if Compare(key, FromString("abcdeg")) >= 0 {
		t.Fatalf("numeric key should sort before greater mismatching string")
	}
	if CompareString(key, "abcdefg") <= 0 || CompareString(key, "abcdefghi") >= 0 {
		t.Fatalf("mixed CompareString length ordering mismatch")
	}
}

func TestIndexKeyStringPredicatesNegativeCases(t *testing.T) {
	alpha := FromString("alpha")
	if HasPrefixString(alpha, "alphabet") || HasPrefixString(alpha, "be") {
		t.Fatalf("string prefix negative case failed")
	}
	if HasSuffixString(alpha, "alphabet") || HasSuffixString(alpha, "al") {
		t.Fatalf("string suffix negative case failed")
	}
	if ContainsString(alpha, "alphabet") || ContainsString(alpha, "zz") {
		t.Fatalf("string contains negative case failed")
	}

	raw := FromU64(0x6162636465666768)
	if HasPrefixString(raw, "abd") || HasSuffixString(raw, "efx") || ContainsString(raw, "ce") {
		t.Fatalf("numeric raw-8 predicate negative case failed")
	}
}

func TestPrefixUpperBoundBoundaryComparisons(t *testing.T) {
	upper, ok := NewPrefixUpperBound("az\xff")
	if !ok {
		t.Fatalf("expected upper bound")
	}
	if CompareStringPrefixUpperBound("a", upper) >= 0 {
		t.Fatalf("shorter string must sort below upper bound")
	}
	if CompareStringPrefixUpperBound("a{", upper) != 0 {
		t.Fatalf("exact upper bound mismatch")
	}
	if CompareStringPrefixUpperBound("a{0", upper) <= 0 {
		t.Fatalf("longer string with upper prefix must sort after upper bound")
	}
	if CompareStringPrefixUpperBound("az\xff", upper) >= 0 {
		t.Fatalf("prefix member must sort below upper bound")
	}

	if ComparePrefixUpperBound(FromString("a"), upper) >= 0 {
		t.Fatalf("shorter key must sort below upper bound")
	}
	if ComparePrefixUpperBound(FromString("a{"), upper) != 0 {
		t.Fatalf("exact key upper bound mismatch")
	}
	if ComparePrefixUpperBound(FromString("a{0"), upper) <= 0 {
		t.Fatalf("longer key with upper prefix must sort after upper bound")
	}

	numericUpper, ok := NewPrefixUpperBound("\x01\x02\xff")
	if !ok {
		t.Fatalf("expected numeric upper bound")
	}
	if ComparePrefixUpperBound(FromU64(0x0102ff0000000000), numericUpper) >= 0 {
		t.Fatalf("numeric prefix member must sort below upper bound")
	}
	if ComparePrefixUpperBound(FromU64(0x0103000000000000), numericUpper) <= 0 {
		t.Fatalf("numeric key equal to upper prefix and longer than it must sort after upper bound")
	}
}

func TestDBKeyBytesAndRoundTrip(t *testing.T) {
	var buf [8]byte
	num := DataKeyFromUserKey(uint64(0x0102030405060708), false)
	if got := num.Bytes(false, &buf); !slices.Equal(got, []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("numeric bytes mismatch: %x", got)
	}
	if got := num.Format(false); got != "72623859790382856" {
		t.Fatalf("numeric format: got=%q", got)
	}
	if got := UserKeyFromDataKey[uint64](num, false); got != 0x0102030405060708 {
		t.Fatalf("numeric id roundtrip: got=%x", got)
	}
	if got := num.Uint(); got != 0x0102030405060708 {
		t.Fatalf("numeric carrier Uint: got=%x", got)
	}

	str := DataKeyFromUserKey("user-1", true)
	raw := str.Bytes(true, &buf)
	if string(raw) != "user-1" {
		t.Fatalf("string bytes: got=%q", string(raw))
	}
	if got := str.Format(true); got != "user-1" {
		t.Fatalf("string format: got=%q", got)
	}
	if got := UserKeyFromDataKey[string](str, true); got != "user-1" {
		t.Fatalf("string id roundtrip: got=%q", got)
	}
	if got := str.String(); got != "user-1" {
		t.Fatalf("string carrier String: got=%q", got)
	}
}

func TestOrderedNumericByteStrings(t *testing.T) {
	neg := Int64ByteString(-1)
	pos := Int64ByteString(1)
	if neg >= pos {
		t.Fatalf("ordered int64 bytes out of order")
	}
	low := Float64ByteString(-1.5)
	high := Float64ByteString(2.25)
	if low >= high {
		t.Fatalf("ordered float64 bytes out of order")
	}
	if CanonicalizeFloat64ForIndex(-0) != 0 {
		t.Fatalf("negative zero must canonicalize to zero")
	}
}

func TestNumericKeyFormattingAndOrdering(t *testing.T) {
	if got := U64Bytes(0x0102030405060708); !slices.Equal(got, []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("U64Bytes mismatch: %x", got)
	}
	if got := U64ByteString(0x0102030405060708); got != "\x01\x02\x03\x04\x05\x06\x07\x08" {
		t.Fatalf("U64ByteString mismatch: %x", []byte(got))
	}
	if OrderedInt64Key(math.MinInt64) >= OrderedInt64Key(0) || OrderedInt64Key(0) >= OrderedInt64Key(math.MaxInt64) {
		t.Fatalf("ordered int64 key order mismatch")
	}
	if OrderedFloat64Key(math.Inf(-1)) >= OrderedFloat64Key(-1) || OrderedFloat64Key(1) >= OrderedFloat64Key(math.Inf(1)) {
		t.Fatalf("ordered float64 infinity order mismatch")
	}

	nan := CanonicalizeFloat64ForIndex(math.NaN())
	if !math.IsNaN(nan) || math.Float64bits(nan) != CanonicalFloat64NaNBits {
		t.Fatalf("canonical NaN mismatch: %x", math.Float64bits(nan))
	}
	if math.Float64bits(CanonicalizeFloat64ForIndex(math.Copysign(0, -1))) != 0 {
		t.Fatalf("negative zero bits must canonicalize to positive zero")
	}
	if OrderedFloat64Key(math.NaN()) <= OrderedFloat64Key(math.Inf(1)) {
		t.Fatalf("canonical NaN must sort after +Inf")
	}
}

func TestIDBytesAndRoundTripWithKind(t *testing.T) {
	var buf [8]byte
	if got := UserKeyBytesWithBuf(uint64(0x0102030405060708), false, &buf); !slices.Equal(got, []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("numeric ID bytes mismatch: %x", got)
	}
	if got := UserKeyFromBytes[uint64]([]byte{1, 2, 3, 4, 5, 6, 7, 8}, false); got != 0x0102030405060708 {
		t.Fatalf("numeric IDFromBytesWithKind mismatch: %x", got)
	}

	raw := []byte("user-1")
	if got := string(UserKeyBytesWithBuf("user-1", true, &buf)); got != "user-1" {
		t.Fatalf("string ID bytes mismatch: %q", got)
	}
	got := UserKeyFromBytes[string](raw, true)
	raw[0] = 'X'
	if got != "user-1" {
		t.Fatalf("string IDFromBytesWithKind must copy source bytes: got=%q", got)
	}
}

func TestDBKeyBytesNoAllocation(t *testing.T) {
	num := DataKeyFromUserKey(uint64(42), false)
	var buf [8]byte
	if allocs := testing.AllocsPerRun(1000, func() {
		benchBytes = num.Bytes(false, &buf)
	}); allocs != 0 {
		t.Fatalf("numeric DBKey.Bytes allocated: %.2f", allocs)
	}

	str := DataKeyFromUserKey("user-42", true)
	if allocs := testing.AllocsPerRun(1000, func() {
		benchBytes = str.Bytes(true, &buf)
	}); allocs != 0 {
		t.Fatalf("string DBKey.Bytes allocated: %.2f", allocs)
	}

	if allocs := testing.AllocsPerRun(1000, func() {
		benchBytes = UserKeyBytesWithBuf(uint64(42), false, &buf)
	}); allocs != 0 {
		t.Fatalf("numeric IDBytesWithKindBuf allocated: %.2f", allocs)
	}
}

func BenchmarkIndexKeyCompareNumeric(b *testing.B) {
	a := FromU64(100)
	c := FromU64(200)
	b.ReportAllocs()
	for b.Loop() {
		benchInt = Compare(a, c)
	}
}

func BenchmarkIndexKeyCompareString(b *testing.B) {
	a := benchIndexKeyA
	c := benchIndexKeyB
	b.ReportAllocs()
	for b.Loop() {
		benchInt = Compare(a, c)
	}
}

func BenchmarkIndexKeyHasPrefixRaw8(b *testing.B) {
	key := FromU64(0x6162636465666768)
	b.ReportAllocs()
	for b.Loop() {
		benchBool = HasPrefixString(key, "abc")
	}
}

func BenchmarkPrefixUpperBoundCompare(b *testing.B) {
	upper, _ := NewPrefixUpperBound("member:0000000000")
	key := benchIndexKeyA
	b.ReportAllocs()
	for b.Loop() {
		benchInt = ComparePrefixUpperBound(key, upper)
	}
}

func BenchmarkFixed8StringToU64(b *testing.B) {
	s := "\x01\x02\x03\x04\x05\x06\x07\x08"
	b.ReportAllocs()
	for b.Loop() {
		benchUint64 = Fixed8StringToU64(s)
	}
}

func BenchmarkIndexKeyCompareToString(b *testing.B) {
	key := benchIndexKeyA
	b.ReportAllocs()
	for b.Loop() {
		benchInt = CompareString(key, "member:000000000099")
	}
}

func BenchmarkIndexKeyEqualsString(b *testing.B) {
	key := benchIndexKeyA
	b.ReportAllocs()
	for b.Loop() {
		benchBool = EqualsString(key, "member:000000000001")
	}
}

func BenchmarkIndexKeyHasSuffixString(b *testing.B) {
	key := benchIndexKeyA
	b.ReportAllocs()
	for b.Loop() {
		benchBool = HasSuffixString(key, "0001")
	}
}

func BenchmarkIndexKeyContainsString(b *testing.B) {
	key := benchIndexKeyA
	b.ReportAllocs()
	for b.Loop() {
		benchBool = ContainsString(key, "000000")
	}
}

func BenchmarkStringPrefixUpperBoundCompare(b *testing.B) {
	upper, _ := NewPrefixUpperBound("member:0000000000")
	b.ReportAllocs()
	for b.Loop() {
		benchInt = CompareStringPrefixUpperBound("member:000000000001", upper)
	}
}

func BenchmarkDBKeyBytesUint64(b *testing.B) {
	b.ReportAllocs()
	key := benchDBKey
	var buf [8]byte
	for b.Loop() {
		benchBytes = key.Bytes(false, &buf)
	}
}

func BenchmarkIDBytesWithKindBufUint64(b *testing.B) {
	b.ReportAllocs()
	var buf [8]byte
	for i := 0; i < b.N; i++ {
		benchBytes = UserKeyBytesWithBuf(uint64(i), false, &buf)
	}
}

func BenchmarkIDBytesWithKindBufString(b *testing.B) {
	b.ReportAllocs()
	var buf [8]byte
	for i := 0; i < b.N; i++ {
		benchBytes = UserKeyBytesWithBuf("member:000000000001", true, &buf)
	}
}

func BenchmarkIDFromBytesWithKindUint64(b *testing.B) {
	b.ReportAllocs()
	raw := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	for b.Loop() {
		benchUint64 = UserKeyFromBytes[uint64](raw, false)
	}
}

func BenchmarkIDFromBytesWithKindString(b *testing.B) {
	raw := []byte("member:000000000001")
	b.ReportAllocs()
	for b.Loop() {
		benchString = UserKeyFromBytes[string](raw, true)
	}
}

func BenchmarkDBKeyBytesString(b *testing.B) {
	key := DataKeyFromUserKey("member:000000000001", true)
	var buf [8]byte
	b.ReportAllocs()
	for b.Loop() {
		benchBytes = key.Bytes(true, &buf)
	}
}

func BenchmarkU64ByteString(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchString = U64ByteString(uint64(i))
	}
}

func BenchmarkOrderedInt64Key(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchUint64 = OrderedInt64Key(int64(i))
	}
}

func BenchmarkOrderedFloat64Key(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchUint64 = OrderedFloat64Key(float64(i))
	}
}
