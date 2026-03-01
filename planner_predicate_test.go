package rbi

import (
	"fmt"
	"testing"

	"github.com/vapstack/qx"
)

func TestBuildPredRange_PrefixMaterializationStoredInCache(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	for i := 0; i < 700; i++ {
		err := db.Set(uint64(i+1), &Rec{
			Name:  fmt.Sprintf("u_%03d", i),
			Age:   i + 1,
			Email: fmt.Sprintf("user%03d@example.com", i),
		})
		if err != nil {
			t.Fatalf("seed Set(%d): %v", i+1, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "user1"}
	cacheKey := db.materializedPredCacheKey(expr)
	if cacheKey == "" {
		t.Fatalf("expected non-empty materialized cache key for prefix predicate")
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
		t.Fatalf("unexpected cache hit before predicate evaluation")
	}

	fm := db.fields["email"]
	if fm == nil {
		t.Fatalf("expected field metadata for email")
	}
	slice := db.snapshotFieldIndexSlice("email")
	if slice == nil {
		t.Fatalf("expected field index slice for email")
	}

	p, ok := db.buildPredRange(expr, fm, slice)
	if !ok {
		t.Fatalf("expected range/prefix predicate build to succeed")
	}
	if !p.hasContains() {
		t.Fatalf("expected contains predicate for prefix path")
	}
	for i := 1; i <= 90; i++ {
		_ = p.matches(uint64(i))
	}
	releasePredicates([]predicate{p})

	cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey)
	if !ok || cached == nil || cached.IsEmpty() {
		t.Fatalf("expected cached materialized bitmap for prefix predicate")
	}
}

func TestBuildPredRange_PrefixMaterializationSkippedWhenCacheDisabled(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
	})

	for i := 0; i < 700; i++ {
		err := db.Set(uint64(i+1), &Rec{
			Name:  fmt.Sprintf("u_%03d", i),
			Age:   i + 1,
			Email: fmt.Sprintf("user%03d@example.com", i),
		})
		if err != nil {
			t.Fatalf("seed Set(%d): %v", i+1, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "user1"}
	if cacheKey := db.materializedPredCacheKey(expr); cacheKey != "" {
		t.Fatalf("expected empty materialized cache key when cache is disabled, got %q", cacheKey)
	}

	fm := db.fields["email"]
	if fm == nil {
		t.Fatalf("expected field metadata for email")
	}
	slice := db.snapshotFieldIndexSlice("email")
	if slice == nil {
		t.Fatalf("expected field index slice for email")
	}

	p, ok := db.buildPredRange(expr, fm, slice)
	if !ok {
		t.Fatalf("expected range/prefix predicate build to succeed")
	}
	if !p.hasContains() {
		t.Fatalf("expected contains predicate for prefix path")
	}
	for i := 1; i <= 90; i++ {
		_ = p.matches(uint64(i))
	}
	releasePredicates([]predicate{p})

	rawKey := materializedPredCacheKeyFromScalar("email", qx.OpPREFIX, "user1")
	if _, ok := db.getSnapshot().matPredCache.Load(rawKey); ok {
		t.Fatalf("expected no cache store when materialized predicate cache is disabled")
	}
	if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
		t.Fatalf("expected zero materialized cache entries, got %d", got)
	}
}

func TestMaterializedPredCacheKeyFromScalar_SupportsRangeAndPrefix(t *testing.T) {
	type tc struct {
		op      qx.Op
		wantHit bool
	}
	cases := []tc{
		{op: qx.OpGT, wantHit: true},
		{op: qx.OpGTE, wantHit: true},
		{op: qx.OpLT, wantHit: true},
		{op: qx.OpLTE, wantHit: true},
		{op: qx.OpPREFIX, wantHit: true},
		{op: qx.OpSUFFIX, wantHit: true},
		{op: qx.OpCONTAINS, wantHit: true},
		{op: qx.OpEQ, wantHit: false},
		{op: qx.OpIN, wantHit: false},
	}
	for _, c := range cases {
		got := materializedPredCacheKeyFromScalar("f", c.op, "v")
		if c.wantHit && got == "" {
			t.Fatalf("expected non-empty key for op=%v", c.op)
		}
		if !c.wantHit && got != "" {
			t.Fatalf("expected empty key for op=%v", c.op)
		}
	}
}
