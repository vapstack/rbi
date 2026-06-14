package qcache

import (
	"testing"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/qir"
)

func TestMaterializedPredKeyExactScalarRange_Numeric(t *testing.T) {
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
	if key.String() == "" {
		t.Fatal("expected printable exact range cache key")
	}
}

func TestMaterializedPredKeyExactScalarRangeComplement_Numeric(t *testing.T) {
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
	if key.String() == "" {
		t.Fatal("expected printable exact range complement cache key")
	}
}

func TestMaterializedPredKeyExactScalarRange_PrefixBoundsDoNotCollide(t *testing.T) {
	boundsB := indexdata.Bounds{
		Has:       true,
		HasLo:     true,
		LoKey:     "a",
		LoInc:     true,
		HasHi:     true,
		HiKey:     "z",
		HasPrefix: true,
		Prefix:    "b",
	}
	boundsC := boundsB
	boundsC.Prefix = "c"

	keyB := MaterializedPredKeyForExactScalarRange("name", boundsB)
	keyC := MaterializedPredKeyForExactScalarRange("name", boundsC)
	if keyB.IsZero() || keyC.IsZero() {
		t.Fatal("expected non-zero prefixed exact range cache keys")
	}
	if keyB == keyC {
		t.Fatal("expected different prefixes to produce distinct exact range cache keys")
	}
	if keyB.hash() == keyC.hash() {
		t.Fatal("expected different prefixes to produce distinct exact range cache hashes")
	}
	if keyB.String() == keyC.String() {
		t.Fatal("expected different prefixes to produce distinct printable exact range cache keys")
	}

	compB := MaterializedPredComplementKeyForExactScalarRange("name", boundsB)
	compC := MaterializedPredComplementKeyForExactScalarRange("name", boundsC)
	if compB == compC {
		t.Fatal("expected different prefixes to produce distinct exact range complement cache keys")
	}
	if compB.hash() == compC.hash() {
		t.Fatal("expected different prefixes to produce distinct exact range complement cache hashes")
	}
}

func TestMaterializedPredKeyScalar_NumericBoundsDoNotCollide(t *testing.T) {
	k30 := MaterializedPredKeyForNumericScalar("age", qir.OpGTE, keycodec.FromU64(keycodec.OrderedInt64Key(30)))
	k40 := MaterializedPredKeyForNumericScalar("age", qir.OpGTE, keycodec.FromU64(keycodec.OrderedInt64Key(40)))
	if k30 == k40 {
		t.Fatal("expected distinct numeric scalar cache keys")
	}

	c30 := MaterializedPredComplementKeyForNumericScalar("age", qir.OpGTE, keycodec.FromU64(keycodec.OrderedInt64Key(30)))
	c40 := MaterializedPredComplementKeyForNumericScalar("age", qir.OpGTE, keycodec.FromU64(keycodec.OrderedInt64Key(40)))
	if c30 == c40 {
		t.Fatal("expected distinct numeric scalar complement cache keys")
	}
}

func TestMaterializedPredKeyLookup_Constructors(t *testing.T) {
	str := MaterializedPredKeyForLookupKey("email", qir.OpPREFIX, keycodec.IndexLookupString("user"))
	if str.IsZero() || str.Field() != "email" {
		t.Fatalf("unexpected string lookup key: %#v", str)
	}

	num := MaterializedPredKeyForLookupKey("age", qir.OpGTE, keycodec.IndexLookupU64(keycodec.OrderedInt64Key(30)))
	if num.IsZero() || num.Field() != "age" {
		t.Fatalf("unexpected numeric lookup key: %#v", num)
	}

	comp := MaterializedPredComplementKeyForLookupKey("age", qir.OpGTE, keycodec.IndexLookupU64(keycodec.OrderedInt64Key(30)))
	if comp.IsZero() || comp.Field() != "age" {
		t.Fatalf("unexpected numeric complement lookup key: %#v", comp)
	}
	if str == num || num == comp {
		t.Fatalf("expected lookup key variants to stay distinct")
	}
}

func TestMaterializedPredKeyDistinctSet(t *testing.T) {
	vals := keycodec.GetIndexLookupKeySlice(2)
	defer keycodec.ReleaseIndexLookupKeySlice(vals)
	vals = append(vals, keycodec.IndexLookupString("DE"), keycodec.IndexLookupString("FR"))

	key := MaterializedPredKeyForDistinctLookupKeys("country", qir.OpIN, vals, true)
	if key.IsZero() {
		t.Fatal("expected non-zero distinct-set cache key")
	}
	if key.String() == "" {
		t.Fatal("expected printable distinct-set cache key")
	}

	numeric := MaterializedPredKeyForDistinctLookupKeys("age", qir.OpIN, []keycodec.IndexLookupKey{
		keycodec.IndexLookupU64(keycodec.OrderedInt64Key(18)),
		keycodec.IndexLookupU64(keycodec.OrderedInt64Key(21)),
	}, false)
	if numeric.IsZero() || numeric == key {
		t.Fatalf("unexpected numeric distinct-set cache key: %#v", numeric)
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
