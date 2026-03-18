//go:build rbidebug

package roaring

import "testing"

func TestBitmapCloneReusesBitmapAndRunPools(t *testing.T) {
	src := NewBitmap()
	src.AddRange(0, 10000)
	src.RunOptimize()

	ResetPoolStats()

	first := src.Clone()
	if !first.Equals(src) {
		t.Fatalf("first clone differs from source")
	}
	ReleaseBitmap(first)

	second := src.Clone()
	if !second.Equals(src) {
		t.Fatalf("second clone differs from source")
	}
	ReleaseBitmap(second)

	stats := GetPoolStatsSinceReset()
	if stats.Bitmaps.Puts == 0 {
		t.Fatalf("expected cloned bitmaps to be returned to the bitmap pool")
	}
	if stats.Bitmaps.Hits == 0 {
		t.Fatalf("expected bitmap clone to reuse a pooled bitmap")
	}
	if stats.RunContainers.Puts == 0 {
		t.Fatalf("expected run containers to be returned to the pool")
	}
	if stats.RunContainers.Hits == 0 {
		t.Fatalf("expected run container clone to reuse a pooled run container")
	}
}
