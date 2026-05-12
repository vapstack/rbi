package qcache

import (
	"testing"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/qir"
)

func TestMaterializedPredKeyExactScalarRange_RoundTripNumeric(t *testing.T) {
	bounds := indexdata.Bounds{
		Has:       true,
		HasLo:     true,
		LoInc:     true,
		LoNumeric: true,
		LoIndex:   keycodec.FromU64(keycodec.OrderedInt64Key(30)),
		HasHi:     true,
		HiInc:     false,
		HiNumeric: true,
		HiIndex:   keycodec.FromU64(keycodec.OrderedInt64Key(60)),
	}

	key := MaterializedPredKeyForExactScalarRange("age", bounds)
	if key.IsZero() {
		t.Fatal("expected non-zero exact range cache key")
	}

	parsed, ok := MaterializedPredKeyFromEncoded(key.String())
	if !ok {
		t.Fatal("expected numeric exact range cache key to parse")
	}
	if parsed != key {
		t.Fatalf("round-trip mismatch:\n got=%#v\nwant=%#v", parsed, key)
	}
}

func TestMaterializedPredKeyExactScalarRangeComplement_RoundTripNumeric(t *testing.T) {
	bounds := indexdata.Bounds{
		Has:       true,
		HasLo:     true,
		LoInc:     true,
		LoNumeric: true,
		LoIndex:   keycodec.FromU64(keycodec.OrderedInt64Key(30)),
		HasHi:     true,
		HiInc:     false,
		HiNumeric: true,
		HiIndex:   keycodec.FromU64(keycodec.OrderedInt64Key(60)),
	}

	key := MaterializedPredComplementKeyForExactScalarRange("age", bounds)
	if key.IsZero() {
		t.Fatal("expected non-zero exact range complement cache key")
	}

	parsed, ok := MaterializedPredKeyFromEncoded(key.String())
	if !ok {
		t.Fatal("expected numeric exact range complement cache key to parse")
	}
	if parsed != key {
		t.Fatalf("round-trip mismatch:\n got=%#v\nwant=%#v", parsed, key)
	}
}

func TestMaterializedPredKeyExactScalarRange_ParseLegacyEncoding(t *testing.T) {
	legacy := "age\x1frange_exact\x1f[\x00\x00\x00\x00\x00\x00\x00\x1e\x1f)\x00\x00\x00\x00\x00\x00\x00<"
	parsed, ok := MaterializedPredKeyFromEncoded(legacy)
	if !ok {
		t.Fatal("expected legacy exact range cache key to parse")
	}
	if parsed.kind != materializedPredKeyExactScalarRange {
		t.Fatalf("unexpected kind=%v", parsed.kind)
	}
	if parsed.flags&materializedPredKeyHasLo == 0 || parsed.flags&materializedPredKeyHasHi == 0 {
		t.Fatalf("expected both bounds, flags=%08b", parsed.flags)
	}
	if parsed.flags&(materializedPredKeyLoNumeric|materializedPredKeyHiNumeric) != 0 {
		t.Fatalf("legacy parse should stay string-based, flags=%08b", parsed.flags)
	}
}

func TestMaterializedPredKeyScalar_NumericBoundsDoNotCollide(t *testing.T) {
	k30 := MaterializedPredKeyForNumericScalar("age", qir.OpGTE, keycodec.FromU64(keycodec.OrderedInt64Key(30)))
	k40 := MaterializedPredKeyForNumericScalar("age", qir.OpGTE, keycodec.FromU64(keycodec.OrderedInt64Key(40)))
	if k30 == k40 {
		t.Fatal("expected distinct numeric scalar cache keys")
	}

	p30, ok := MaterializedPredKeyFromEncoded(k30.String())
	if !ok {
		t.Fatal("expected numeric scalar cache key to parse")
	}
	if p30 != k30 {
		t.Fatalf("scalar round-trip mismatch:\n got=%#v\nwant=%#v", p30, k30)
	}

	c30 := MaterializedPredComplementKeyForNumericScalar("age", qir.OpGTE, keycodec.FromU64(keycodec.OrderedInt64Key(30)))
	c40 := MaterializedPredComplementKeyForNumericScalar("age", qir.OpGTE, keycodec.FromU64(keycodec.OrderedInt64Key(40)))
	if c30 == c40 {
		t.Fatal("expected distinct numeric scalar complement cache keys")
	}
}

func TestMaterializedPredKeyLookup_RoundTrip(t *testing.T) {
	str := MaterializedPredKeyForLookupKey("email", qir.OpPREFIX, keycodec.IndexLookupString("user"))
	if str.IsZero() || str.Field() != "email" {
		t.Fatalf("unexpected string lookup key: %#v", str)
	}
	parsedStr, ok := MaterializedPredKeyFromEncoded(str.String())
	if !ok || parsedStr != str {
		t.Fatalf("string lookup round-trip mismatch:\n got=%#v\nwant=%#v", parsedStr, str)
	}

	num := MaterializedPredKeyForLookupKey("age", qir.OpGTE, keycodec.IndexLookupU64(keycodec.OrderedInt64Key(30)))
	parsedNum, ok := MaterializedPredKeyFromEncoded(num.String())
	if !ok || parsedNum != num {
		t.Fatalf("numeric lookup round-trip mismatch:\n got=%#v\nwant=%#v", parsedNum, num)
	}

	comp := MaterializedPredComplementKeyForLookupKey("age", qir.OpGTE, keycodec.IndexLookupU64(keycodec.OrderedInt64Key(30)))
	parsedComp, ok := MaterializedPredKeyFromEncoded(comp.String())
	if !ok || parsedComp != comp {
		t.Fatalf("numeric complement lookup round-trip mismatch:\n got=%#v\nwant=%#v", parsedComp, comp)
	}
}

func TestMaterializedPredKeyDistinctSet_RoundTrip(t *testing.T) {
	vals := pooled.GetStringSlice(2)
	defer pooled.ReleaseStringSlice(vals)
	vals = append(vals, "DE", "FR")

	key := MaterializedPredKeyForDistinctSetTerms("country", qir.OpIN, vals, true)
	if key.IsZero() {
		t.Fatal("expected non-zero distinct-set cache key")
	}

	parsed, ok := MaterializedPredKeyFromEncoded(key.String())
	if !ok {
		t.Fatal("expected distinct-set cache key to parse")
	}
	if parsed != key {
		t.Fatalf("distinct-set round-trip mismatch:\n got=%#v\nwant=%#v", parsed, key)
	}

}

func TestMaterializedPredKeySlicePool(t *testing.T) {
	keys := GetMaterializedPredKeySlice(2)
	keys = append(keys,
		MaterializedPredKeyForScalar("email", qir.OpPREFIX, "a"),
		MaterializedPredKeyForScalar("name", qir.OpSUFFIX, "b"),
	)
	if len(keys) != 2 {
		t.Fatalf("unexpected key slice len=%d", len(keys))
	}
	ReleaseMaterializedPredKeySlice(keys)
}
