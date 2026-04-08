package rbi

import (
	"testing"

	"github.com/vapstack/qx"
)

func TestMaterializedPredKeyExactScalarRange_RoundTripNumeric(t *testing.T) {
	bounds := rangeBounds{
		has:       true,
		hasLo:     true,
		loInc:     true,
		loNumeric: true,
		loIndex:   indexKeyFromU64(orderedInt64Key(30)),
		hasHi:     true,
		hiInc:     false,
		hiNumeric: true,
		hiIndex:   indexKeyFromU64(orderedInt64Key(60)),
	}

	key := materializedPredKeyForExactScalarRange("age", bounds)
	if key.isZero() {
		t.Fatal("expected non-zero exact range cache key")
	}

	parsed, ok := materializedPredKeyFromEncoded(key.String())
	if !ok {
		t.Fatal("expected numeric exact range cache key to parse")
	}
	if parsed != key {
		t.Fatalf("round-trip mismatch:\n got=%#v\nwant=%#v", parsed, key)
	}
}

func TestMaterializedPredKeyExactScalarRange_ParseLegacyEncoding(t *testing.T) {
	legacy := "age\x1frange_exact\x1f[\x00\x00\x00\x00\x00\x00\x00\x1e\x1f)\x00\x00\x00\x00\x00\x00\x00<"
	parsed, ok := materializedPredKeyFromEncoded(legacy)
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
	k30 := materializedPredKeyForNumericScalar("age", qx.OpGTE, indexKeyFromU64(orderedInt64Key(30)))
	k40 := materializedPredKeyForNumericScalar("age", qx.OpGTE, indexKeyFromU64(orderedInt64Key(40)))
	if k30 == k40 {
		t.Fatal("expected distinct numeric scalar cache keys")
	}

	p30, ok := materializedPredKeyFromEncoded(k30.String())
	if !ok {
		t.Fatal("expected numeric scalar cache key to parse")
	}
	if p30 != k30 {
		t.Fatalf("scalar round-trip mismatch:\n got=%#v\nwant=%#v", p30, k30)
	}

	c30 := materializedPredComplementKeyForNumericScalar("age", qx.OpGTE, indexKeyFromU64(orderedInt64Key(30)))
	c40 := materializedPredComplementKeyForNumericScalar("age", qx.OpGTE, indexKeyFromU64(orderedInt64Key(40)))
	if c30 == c40 {
		t.Fatal("expected distinct numeric scalar complement cache keys")
	}
}
