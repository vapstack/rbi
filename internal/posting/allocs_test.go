package posting

import "testing"

var (
	allocBoolSink              bool
	allocUint32Sink            uint32
	allocUint64Sink            uint64
	allocContainerIndexScratch containerIndex
	allocLargeArrayScratch     largeArray
)

func requireZeroAllocsAfterPoolWarmup(t *testing.T, fn func()) {
	t.Helper()
	if testRaceEnabled {
		t.Skip("allocation guard is disabled under -race because sync.Pool intentionally drops cached objects there")
	}
	for i := 0; i < 32; i++ {
		fn()
	}
	allocs := testing.AllocsPerRun(1000, fn)
	if allocs != 0 {
		t.Fatalf("unexpected allocations after pool warmup: got=%v want=0", allocs)
	}
}

func buildTestLargeIDs(highCount, perHigh int, offset uint64) []uint64 {
	ids := make([]uint64, 0, highCount*perHigh)
	for high := 0; high < highCount; high++ {
		base := (uint64(high) + offset) << 32
		for i := 0; i < perHigh; i++ {
			ids = append(ids, base+uint64(i*2+1))
		}
	}
	return ids
}

func buildTestBitmap32IDs(highCount, perHigh int, offset uint32) []uint32 {
	ids := make([]uint32, 0, highCount*perHigh)
	for high := 0; high < highCount; high++ {
		base := (uint32(high) + offset) << 16
		for i := 0; i < perHigh; i++ {
			ids = append(ids, base+uint32(i*2+1))
		}
	}
	return ids
}

func TestHotPathPools_NoAllocsAfterWarmup(t *testing.T) {
	smallIDs := []uint64{3, 9, 17, 25}
	midIDs := make([]uint64, MidCap)
	for i := range midIDs {
		midIDs[i] = uint64(i*3 + 1)
	}
	largeIDs := buildTestLargeIDs(4, 24, 0)
	largeOtherIDs := buildTestLargeIDs(4, 24, 0)
	for high := 0; high < 4; high++ {
		base := uint64(high) << 32
		for i := 0; i < 24; i++ {
			largeOtherIDs[high*24+i] = base + uint64(i*2+25)
		}
	}
	largeMaskIDs := make([]uint64, 0, len(largeIDs)/2)
	for i := 0; i < len(largeIDs); i += 2 {
		largeMaskIDs = append(largeMaskIDs, largeIDs[i])
	}
	wideLargeIDs := buildTestLargeIDs(8, 24, 0)
	addManySortedIDs := buildTestLargeIDs(2, 12, 8)
	addManyUnsortedIDs := []uint64{
		11<<32 | 19,
		9<<32 | 7,
		10<<32 | 13,
		9<<32 | 3,
		11<<32 | 5,
		10<<32 | 1,
	}

	small := BuildFromSorted(smallIDs)
	defer small.Release()
	mid := BuildFromSorted(midIDs)
	defer mid.Release()
	large := BuildFromSorted(largeIDs)
	defer large.Release()
	largeOther := BuildFromSorted(largeOtherIDs)
	defer largeOther.Release()
	largeMask := BuildFromSorted(largeMaskIDs)
	defer largeMask.Release()
	wideLarge := BuildFromSorted(wideLargeIDs)
	defer wideLarge.Release()

	t.Run("ListBorrowSmall", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			borrowed := small.Borrow()
			allocBoolSink = borrowed.Contains(smallIDs[0])
			borrowed.Release()
		})
	})

	t.Run("ListBorrowLarge", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			borrowed := large.Borrow()
			allocBoolSink = borrowed.Contains(largeIDs[len(largeIDs)/2])
			borrowed.Release()
		})
	})

	t.Run("ListContainsCardinalityMinMax", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocBoolSink = small.Contains(smallIDs[0]) && mid.Contains(midIDs[len(midIDs)/2]) && large.Contains(largeIDs[len(largeIDs)/2])
			allocUint64Sink = small.Cardinality() + mid.Cardinality() + large.Cardinality()
			minID, _ := large.Minimum()
			maxID, _ := large.Maximum()
			allocUint64Sink += minID + maxID
		})
	})

	t.Run("ListIterLarge", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			it := large.Iter()
			var sum uint64
			for it.HasNext() {
				sum += it.Next()
			}
			allocUint64Sink = sum
			it.Release()
		})
	})

	t.Run("ListCloneLarge", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := large.Clone()
			allocUint64Sink = out.Cardinality()
			out.Release()
		})
	})

	t.Run("ListCloneIntoLarge", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			var out List
			out = large.CloneInto(out)
			allocUint64Sink = out.Cardinality()
			out.Release()
		})
	})

	t.Run("ListBuildAddedSmall", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := small.BuildAdded(99)
			allocUint64Sink = out.Cardinality()
			out.Release()
		})
	})

	t.Run("ListBuildRemovedMid", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := mid.BuildRemoved(midIDs[len(midIDs)/2])
			allocUint64Sink = out.Cardinality()
			out.Release()
		})
	})

	t.Run("ListBuildAddedManySortedLarge", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := large.BuildAddedMany(addManySortedIDs)
			allocUint64Sink = out.Cardinality()
			out.Release()
		})
	})

	t.Run("ListBuildAddedManyUnsortedLarge", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := large.BuildAddedMany(addManyUnsortedIDs)
			allocUint64Sink = out.Cardinality()
			out.Release()
		})
	})

	t.Run("ListBuildAndLarge", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := large.BuildAnd(largeMask)
			allocUint64Sink = out.Cardinality()
			out.Release()
		})
	})

	t.Run("ListBuildOrLarge", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := large.BuildOr(largeOther)
			allocUint64Sink = out.Cardinality()
			out.Release()
		})
	})

	t.Run("ListBuildAndNotLarge", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := large.BuildAndNot(largeOther)
			allocUint64Sink = out.Cardinality()
			out.Release()
		})
	})

	t.Run("ListAndCardinalityIntersects", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocUint64Sink = large.AndCardinality(largeOther)
			allocBoolSink = large.Intersects(largeOther)
		})
	})

	t.Run("ListBuildFromSortedLarge", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := BuildFromSorted(largeIDs)
			allocUint64Sink = out.Cardinality()
			out.Release()
		})
	})

	bitmapIDs := buildTestBitmap32IDs(4, 32, 0)
	bitmapOtherIDs := buildTestBitmap32IDs(4, 32, 0)
	for high := 0; high < 4; high++ {
		base := uint32(high) << 16
		for i := 0; i < 32; i++ {
			bitmapOtherIDs[high*32+i] = base + uint32(i*2+17)
		}
	}
	bitmapAddSortedIDs := buildTestBitmap32IDs(2, 24, 8)
	wideBitmapIDs := buildTestBitmap32IDs(8, 32, 0)

	leftBitmap := buildBitmap32(bitmapIDs...)
	defer leftBitmap.Release()
	rightBitmap := buildBitmap32(bitmapOtherIDs...)
	defer rightBitmap.Release()
	wideBitmap := buildBitmap32(wideBitmapIDs...)
	defer wideBitmap.Release()

	t.Run("Bitmap32Contains", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocBoolSink = leftBitmap.contains(bitmapIDs[len(bitmapIDs)/2])
		})
	})

	t.Run("Bitmap32Iterator", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			it := leftBitmap.iterator()
			var sum uint32
			for it.hasNext() {
				sum += it.next()
			}
			allocUint32Sink = sum
			it.release()
		})
	})

	t.Run("Bitmap32Clone", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := leftBitmap.clone()
			allocUint64Sink = out.cardinality()
			out.Release()
		})
	})

	t.Run("ContainerIndexGrowWide", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocContainerIndexScratch.grow(8)
			allocUint64Sink = uint64(allocContainerIndexScratch.size())
			allocContainerIndexScratch.releaseBacking()
		})
	})

	t.Run("ContainerIndexCopyFromWide", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocContainerIndexScratch.copyFrom(&wideBitmap.highlowcontainer)
			allocUint64Sink = uint64(allocContainerIndexScratch.size())
			releaseContainerIndexForTest(&allocContainerIndexScratch)
		})
	})

	t.Run("ContainerIndexCopySharedFromWide", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocContainerIndexScratch.copySharedFrom(&wideBitmap.highlowcontainer)
			allocUint64Sink = uint64(allocContainerIndexScratch.size())
			releaseContainerIndexForTest(&allocContainerIndexScratch)
		})
	})

	containerSample := wideBitmap.highlowcontainer.getContainerAtIndex(0)

	t.Run("ContainerIndexAppendWide", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocContainerIndexScratch.setSize(8)
			for i := 0; i < 8; i++ {
				allocContainerIndexScratch.keys[i] = uint16(i*2 + 1)
				allocContainerIndexScratch.containers[i] = containerSample.retain()
			}
			allocContainerIndexScratch.appendContainer(17, containerSample.retain())
			allocUint64Sink = uint64(allocContainerIndexScratch.size())
			releaseContainerIndexForTest(&allocContainerIndexScratch)
		})
	})

	t.Run("ContainerIndexInsertWide", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocContainerIndexScratch.setSize(8)
			for i := 0; i < 8; i++ {
				allocContainerIndexScratch.keys[i] = uint16(i*2 + 1)
				allocContainerIndexScratch.containers[i] = containerSample.retain()
			}
			allocContainerIndexScratch.insertNewKeyValueAt(4, 8, containerSample.retain())
			allocUint64Sink = uint64(allocContainerIndexScratch.size())
			releaseContainerIndexForTest(&allocContainerIndexScratch)
		})
	})

	t.Run("Bitmap32AddManySorted", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := bitmapPool.Get()
			out.addManySorted(bitmapAddSortedIDs)
			allocUint64Sink = out.cardinality()
			out.Release()
		})
	})

	t.Run("Bitmap32And", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := leftBitmap.clone()
			out.and(rightBitmap)
			allocUint64Sink = out.cardinality()
			out.Release()
		})
	})

	t.Run("Bitmap32Or", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := leftBitmap.clone()
			out.or(rightBitmap)
			allocUint64Sink = out.cardinality()
			out.Release()
		})
	})

	t.Run("Bitmap32AndNot", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			out := leftBitmap.clone()
			out.andNot(rightBitmap)
			allocUint64Sink = out.cardinality()
			out.Release()
		})
	})

	wideLargeRef := wideLarge.largeRef()

	t.Run("LargeArrayGrowWide", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocLargeArrayScratch.grow(8)
			allocUint64Sink = uint64(allocLargeArrayScratch.size())
			allocLargeArrayScratch.releaseBacking()
		})
	})

	t.Run("LargeArrayCopyFromWide", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocLargeArrayScratch.copyFrom(&wideLargeRef.highlowcontainer)
			allocUint64Sink = uint64(allocLargeArrayScratch.size())
			releaseLargeArrayForTest(&allocLargeArrayScratch)
		})
	})

	t.Run("LargeArrayCopySharedFromWide", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocLargeArrayScratch.copySharedFrom(&wideLargeRef.highlowcontainer)
			allocUint64Sink = uint64(allocLargeArrayScratch.size())
			releaseLargeArrayForTest(&allocLargeArrayScratch)
		})
	})

	largeSample := wideLargeRef.highlowcontainer.getContainerAtIndex(0)

	t.Run("LargeArrayAppendWide", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocLargeArrayScratch.setSize(8)
			for i := 0; i < 8; i++ {
				allocLargeArrayScratch.keys[i] = uint32(i*2 + 1)
				allocLargeArrayScratch.containers[i] = largeSample.retain()
			}
			allocLargeArrayScratch.appendContainer(17, largeSample.retain())
			allocUint64Sink = uint64(allocLargeArrayScratch.size())
			releaseLargeArrayForTest(&allocLargeArrayScratch)
		})
	})

	t.Run("LargeArrayInsertWide", func(t *testing.T) {
		requireZeroAllocsAfterPoolWarmup(t, func() {
			allocLargeArrayScratch.setSize(8)
			for i := 0; i < 8; i++ {
				allocLargeArrayScratch.keys[i] = uint32(i*2 + 1)
				allocLargeArrayScratch.containers[i] = largeSample.retain()
			}
			allocLargeArrayScratch.insertNewKeyValueAt(4, 8, largeSample.retain())
			allocUint64Sink = uint64(allocLargeArrayScratch.size())
			releaseLargeArrayForTest(&allocLargeArrayScratch)
		})
	})
}
