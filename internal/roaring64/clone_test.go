package roaring64

import "testing"

func TestBitmapCloneIntoOverwritesAndKeepsDeepCopy(t *testing.T) {
	src := NewBitmap()
	src.Add(1)
	src.Add((1 << 32) | 7)
	src.Add((2 << 32) | 11)
	src.Add((2 << 32) | 12)
	src.RunOptimize()

	dst := NewBitmap()
	dst.Add(99)
	dst.Add((5 << 32) | 3)

	got := src.CloneInto(dst)
	if got != dst {
		t.Fatalf("CloneInto should return destination bitmap")
	}
	if !dst.Equals(src) {
		t.Fatalf("CloneInto result differs from source")
	}

	dst.Add((9 << 32) | 1)
	if src.Contains((9 << 32) | 1) {
		t.Fatalf("CloneInto result shares state with source")
	}

	empty := NewBitmap()
	empty.CloneInto(dst)
	if !dst.IsEmpty() {
		t.Fatalf("CloneInto should clear destination when source is empty")
	}
}

func TestBitmapCloneIntoAliasedDestinationKeepsSourceIntact(t *testing.T) {
	src := NewBitmap()
	src.Add(1)
	src.Add((1 << 32) | 3)
	src.Add((2 << 32) | 5)
	src.RunOptimize()

	want := src.Clone()

	aliasValue := *src
	dst := &aliasValue

	got := src.CloneInto(dst)
	if got != dst {
		t.Fatalf("CloneInto should return destination bitmap")
	}
	if !dst.Equals(src) {
		t.Fatalf("CloneInto result differs from source")
	}
	if !src.Equals(want) {
		t.Fatalf("CloneInto corrupted source bitmap when destination aliased it")
	}

	dst.Add((9 << 32) | 7)
	if src.Contains((9 << 32) | 7) {
		t.Fatalf("aliased CloneInto result still shares state with source")
	}
}
