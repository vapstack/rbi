package posting

import (
	"slices"
	"testing"
)

func containerRangeUint16(start, endx int) []uint16 {
	if start >= endx {
		return nil
	}
	out := make([]uint16, endx-start)
	for i := range out {
		out[i] = uint16(start + i)
	}
	return out
}

func singletonIntervals16(values []uint16) []interval16 {
	out := make([]interval16, len(values))
	for i, v := range values {
		out[i] = newInterval16Range(v, v)
	}
	return out
}

func assertRunIntervalsInvariant(t *testing.T, rc *containerRun) {
	t.Helper()
	for i := 1; i < len(rc.iv); i++ {
		prev := rc.iv[i-1]
		cur := rc.iv[i]
		if prev.start > cur.start {
			t.Fatalf("intervals are not sorted: %v", rc.iv)
		}
		if prev.last() >= cur.start {
			t.Fatalf("intervals overlap: %v", rc.iv)
		}
		if prev.last()+1 == cur.start {
			t.Fatalf("intervals are adjacent and should have been merged: %v", rc.iv)
		}
	}
}

func TestContainerRunConstructionAndIntervalHelpers(t *testing.T) {
	rc := newContainerRunRange(10, 19)
	defer releaseContainerRun(rc)
	assertRunIntervalsInvariant(t, rc)
	assertSameContainerSet(t, rc, []uint16{10, 11, 12, 13, 14, 15, 16, 17, 18, 19})

	tempArray := newContainerArrayFromSlice([]uint16{1, 2, 3, 10, 20, 21, 22})
	fromArray := newContainerRunFromArray(tempArray)
	releaseContainerArray(tempArray)
	defer releaseContainerRun(fromArray)
	assertRunIntervalsInvariant(t, fromArray)
	assertSameContainerSet(t, fromArray, []uint16{1, 2, 3, 10, 20, 21, 22})

	bitmap := newContainerBitmap()
	defer releaseContainerBitmap(bitmap)
	bitmap.iaddRange(100, 110)
	bitmap.iaddRange(200, 205)
	fromBitmap := newContainerRunFromBitmap(bitmap)
	defer releaseContainerRun(fromBitmap)
	assertRunIntervalsInvariant(t, fromBitmap)
	assertSameContainerSet(t, fromBitmap, []uint16{
		100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
		200, 201, 202, 203, 204,
	})

	a := newInterval16Range(10, 20)
	b := newInterval16Range(21, 25)
	if !canMerge16(a, b) {
		t.Fatalf("adjacent intervals must merge")
	}
	if haveOverlap16(a, b) {
		t.Fatalf("adjacent intervals must not overlap")
	}

	merged := mergeInterval16s(a, b)
	if merged.start != 10 || merged.last() != 25 {
		t.Fatalf("mergeInterval16s mismatch: got=%v", merged)
	}

	isect, empty := intersectInterval16s(newInterval16Range(10, 20), newInterval16Range(15, 30))
	if empty || isect.start != 15 || isect.last() != 20 {
		t.Fatalf("intersectInterval16s mismatch: got=%v empty=%v", isect, empty)
	}

	left, removed := newInterval16Range(10, 20).subtractInterval(newInterval16Range(13, 17))
	if removed != 5 || len(left) != 2 || left[0].start != 10 || left[0].last() != 12 || left[1].start != 18 || left[1].last() != 20 {
		t.Fatalf("subtractInterval mismatch: left=%v removed=%d", left, removed)
	}

	inverted := fromArray.invert()
	defer releaseContainerRun(inverted)
	assertRunIntervalsInvariant(t, inverted)
	if inverted.contains(1) || inverted.contains(2) || inverted.contains(3) {
		t.Fatalf("invert must remove existing values")
	}
	if !inverted.contains(0) || !inverted.contains(4) || !inverted.contains(MaxUint16) {
		t.Fatalf("invert must add missing values")
	}
}

func TestContainerRunMutationAndCrossTypeOps(t *testing.T) {
	rc := newContainerRunRange(10, 19)
	defer releaseContainerRun(rc)

	if !rc.iadd(25) || rc.iadd(25) {
		t.Fatalf("iadd duplicate detection mismatch")
	}
	if !rc.iremove(12) || rc.iremove(12) {
		t.Fatalf("iremove mismatch")
	}
	rc.iaddRange(30, 35)
	rc.iremoveRange(15, 18)
	assertRunIntervalsInvariant(t, rc)
	assertSameContainerSet(t, rc, []uint16{
		10, 11, 13, 14, 18, 19, 25,
		30, 31, 32, 33, 34,
	})

	flipped := rc.not(9, 12)
	defer releaseContainer(flipped)
	assertSameContainerSet(t, flipped, []uint16{
		9, 13, 14, 18, 19, 25,
		30, 31, 32, 33, 34,
	})

	inverted := rc.inot(30, 33)
	defer releaseContainerPair(rc, inverted)
	rc = nil
	assertSameContainerSet(t, inverted, []uint16{
		10, 11, 13, 14, 18, 19, 25, 33, 34,
	})

	leftIDs := []uint16{1, 2, 3, 10, 11, 12, 30, 31}
	rightIDs := []uint16{2, 3, 4, 11, 12, 13, 31, 32}
	for _, factory := range containerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			left := buildContainerRun(leftIDs)
			right := factory.build(rightIDs)
			defer releaseContainer(left)
			defer releaseContainer(right)

			unionWant := unionUint16(leftIDs, rightIDs)
			andWant := intersectUint16(leftIDs, rightIDs)
			xorWant := xorUint16(leftIDs, rightIDs)
			diffWant := differenceUint16(leftIDs, rightIDs)

			orResult := left.or(right)
			assertSameContainerSet(t, orResult, unionWant)
			releaseContainer(orResult)

			andResult := left.and(right)
			assertSameContainerSet(t, andResult, andWant)
			releaseContainer(andResult)

			xorResult := left.xor(right)
			assertSameContainerSet(t, xorResult, xorWant)
			releaseContainer(xorResult)

			andNotResult := left.andNot(right)
			assertSameContainerSet(t, andNotResult, diffWant)
			releaseContainer(andNotResult)

			iorLeft := buildContainerRun(leftIDs)
			iorRight := factory.build(rightIDs)
			iorResult := iorLeft.ior(iorRight)
			assertSameContainerSet(t, iorResult, unionWant)
			releaseContainer(iorRight)
			releaseContainerPair(iorLeft, iorResult)

			iandLeft := buildContainerRun(leftIDs)
			iandRight := factory.build(rightIDs)
			iandResult := iandLeft.iand(iandRight)
			assertSameContainerSet(t, iandResult, andWant)
			releaseContainer(iandRight)
			releaseContainerPair(iandLeft, iandResult)

			iandNotLeft := buildContainerRun(leftIDs)
			iandNotRight := factory.build(rightIDs)
			iandNotResult := iandNotLeft.iandNot(iandNotRight)
			assertSameContainerSet(t, iandNotResult, diffWant)
			releaseContainer(iandNotRight)
			releaseContainerPair(iandNotLeft, iandNotResult)

			if got := left.intersects(right); got != (len(andWant) > 0) {
				t.Fatalf("intersects mismatch: got=%v want=%v", got, len(andWant) > 0)
			}
			if got := left.andCardinality(right); got != len(andWant) {
				t.Fatalf("andCardinality mismatch: got=%d want=%d", got, len(andWant))
			}
		})
	}
}

func TestContainerRunNotCopyAndInotRangeToggle(t *testing.T) {
	tests := []struct {
		name       string
		base       []uint16
		start, end int
	}{
		{name: "Empty", start: 5, end: 8},
		{name: "BeforeFirst", base: []uint16{10, 11, 12, 20, 21, 22}, start: 1, end: 4},
		{name: "AfterLast", base: []uint16{10, 11, 12}, start: 20, end: 23},
		{name: "SplitSingleRun", base: containerRangeUint16(10, 21), start: 13, end: 18},
		{name: "BridgeMultipleRuns", base: []uint16{10, 11, 12, 15, 16, 18, 19, 20}, start: 13, end: 18},
		{name: "CoverMiddleRuns", base: []uint16{10, 11, 12, 20, 21, 22, 30, 31, 32}, start: 11, end: 31},
		{name: "TouchesZero", base: []uint16{5, 6, 7}, start: 0, end: 5},
		{name: "TouchesMaxCapacity", base: containerRangeUint16(maxCapacity-6, maxCapacity), start: maxCapacity - 3, end: maxCapacity},
		{name: "FullDomain", base: []uint16{1, 2, 3, 10, 20, 21, 22}, start: 0, end: maxCapacity},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := xorUint16(tc.base, containerRangeUint16(tc.start, tc.end))

			src := buildContainerRun(tc.base).(*containerRun)
			defer releaseContainerRun(src)
			notCopy := src.notCopy(tc.start, tc.end)
			defer releaseContainerRun(notCopy)
			assertRunIntervalsInvariant(t, notCopy)
			assertSameContainerSet(t, notCopy, want)
			assertSameContainerSet(t, src, tc.base)

			notResult := src.not(tc.start, tc.end)
			defer releaseContainer(notResult)
			assertSameContainerSet(t, notResult, want)
			assertSameContainerSet(t, src, tc.base)

			inplace := buildContainerRun(tc.base).(*containerRun)
			inotResult := inplace.inot(tc.start, tc.end)
			assertSameContainerSet(t, inotResult, want)
			if runResult, ok := inotResult.(*containerRun); ok {
				assertRunIntervalsInvariant(t, runResult)
			}
			releaseContainerPair(inplace, inotResult)
		})
	}
}

func TestContainerRunInotEmptyRangeIsNoOp(t *testing.T) {
	rc := ownContainerRun(&containerRun{iv: make([]interval16, 2, 4)})
	rc.iv[0] = newInterval16Range(10, 12)
	rc.iv[1] = newInterval16Range(20, 22)

	beforeBacking := rc.iv[:cap(rc.iv)]
	beforeCap := cap(rc.iv)

	result := rc.inot(15, 15)
	if result != rc {
		t.Fatalf("empty-range inot must return the receiver, got %T", result)
	}
	if !slicesShareBacking(beforeBacking, rc.iv) {
		t.Fatalf("empty-range inot must not replace writable storage")
	}
	if cap(rc.iv) != beforeCap {
		t.Fatalf("empty-range inot changed capacity: got=%d want=%d", cap(rc.iv), beforeCap)
	}
	assertRunIntervalsInvariant(t, rc)
	assertSameContainerSet(t, rc, []uint16{10, 11, 12, 20, 21, 22})
	releaseContainerRun(rc)
}

func TestContainerRunInotReusesWritableStorage(t *testing.T) {
	rc := ownContainerRun(&containerRun{iv: make([]interval16, 2, 4)})
	rc.iv[0] = newInterval16Range(10, 12)
	rc.iv[1] = newInterval16Range(20, 22)

	beforeBacking := rc.iv[:cap(rc.iv)]
	beforeCap := cap(rc.iv)

	result := rc.inot(13, 18)
	if result != rc {
		t.Fatalf("writable inot should keep run identity, got %T", result)
	}
	if !slicesShareBacking(beforeBacking, rc.iv) {
		t.Fatalf("writable inot unexpectedly replaced backing storage")
	}
	if cap(rc.iv) != beforeCap {
		t.Fatalf("writable inot changed capacity: got=%d want=%d", cap(rc.iv), beforeCap)
	}
	assertRunIntervalsInvariant(t, rc)
	assertSameContainerSet(t, rc, []uint16{10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22})
	releaseContainerRun(rc)
}

func TestContainerRunInotReplacesStorageWhenCapacityIsTight(t *testing.T) {
	rc := ownContainerRun(&containerRun{iv: make([]interval16, 2, 2)})
	rc.iv[0] = newInterval16Range(10, 12)
	rc.iv[1] = newInterval16Range(20, 22)

	beforeBacking := rc.iv[:cap(rc.iv)]
	beforeCap := cap(rc.iv)

	result := rc.inot(13, 18)
	if result != rc {
		t.Fatalf("tight-capacity inot should still return the receiver when the result stays run, got %T", result)
	}
	if slicesShareBacking(beforeBacking, rc.iv) {
		t.Fatalf("tight-capacity inot must replace storage via donor path")
	}
	if cap(rc.iv) <= beforeCap {
		t.Fatalf("tight-capacity inot did not grow writable capacity: got=%d want>%d", cap(rc.iv), beforeCap)
	}
	assertRunIntervalsInvariant(t, rc)
	assertSameContainerSet(t, rc, []uint16{10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22})
	releaseContainerRun(rc)
}

func TestContainerRunInotWritablePathCanReturnArray(t *testing.T) {
	rc := ownContainerRun(&containerRun{iv: make([]interval16, 0, 1)})
	beforeBacking := rc.iv[:cap(rc.iv)]

	result := rc.inot(5, 8)
	if _, ok := result.(*containerArray); !ok {
		t.Fatalf("expected array from small flipped range, got %T", result)
	}
	if !slicesShareBacking(beforeBacking, rc.iv) {
		t.Fatalf("small writable inot unexpectedly replaced run backing")
	}
	assertRunIntervalsInvariant(t, rc)
	assertSameContainerSet(t, result, []uint16{5, 6, 7})
	releaseContainerPair(rc, result)
}

func TestContainerRunAndNotBitmapToRunFullSuffixBoundary(t *testing.T) {
	left := newContainerRunRange(1, MaxUint16)
	defer releaseContainerRun(left)

	right := newContainerBitmap()
	defer releaseContainerBitmap(right)
	right.iaddRange(1, maxCapacity)

	result, cardinality := left.andNotBitmapToRun(right)
	defer releaseContainerRun(result)
	if cardinality != 0 {
		t.Fatalf("andNotBitmapToRun cardinality mismatch: got=%d want=0", cardinality)
	}
	if len(result.iv) != 0 {
		t.Fatalf("andNotBitmapToRun must return empty run, got=%v", result.iv)
	}
}

func TestContainerRunIntersectsEdgeCases(t *testing.T) {
	full := newContainerRunRange(0, MaxUint16)
	defer releaseContainerRun(full)

	empty := newContainerArray()
	defer releaseContainerArray(empty)
	if full.intersects(empty) {
		t.Fatalf("full run must not intersect empty container")
	}

	single := buildContainerArray([]uint16{42})
	defer releaseContainer(single)
	if !full.intersects(single) {
		t.Fatalf("full run must intersect non-empty container")
	}

	left := buildContainerRun([]uint16{10, 11, 12, 100, 101, 102}).(*containerRun)
	defer releaseContainerRun(left)

	adjacent := newContainerRunRange(13, 15)
	defer releaseContainerRun(adjacent)
	if left.intersects(adjacent) {
		t.Fatalf("adjacent runs must not intersect")
	}

	arrayHit := buildContainerArray([]uint16{13, 99, 101})
	defer releaseContainer(arrayHit)
	if !left.intersects(arrayHit) {
		t.Fatalf("run/array late-range hit was missed")
	}

	bitmapMiss := buildContainerBitmap([]uint16{50, 60})
	defer releaseContainer(bitmapMiss)
	if left.intersects(bitmapMiss) {
		t.Fatalf("run/bitmap unexpectedly intersected")
	}
}

func TestContainerRunAndNotRunCopyAndXorEmptyRegressions(t *testing.T) {
	nonEmpty := buildContainerRun([]uint16{10, 11, 12, 100, 101}).(*containerRun)
	defer releaseContainerRun(nonEmpty)
	empty := newContainerRun()
	defer releaseContainerRun(empty)

	diff := nonEmpty.andNotRunCopy(empty)
	defer releaseContainerRun(diff)
	if diff == nonEmpty {
		t.Fatalf("andNotRunCopy must return a fresh run when subtracting empty")
	}
	assertRunIntervalsInvariant(t, diff)
	assertSameContainerSet(t, diff, []uint16{10, 11, 12, 100, 101})

	emptyDiff := empty.andNotRunCopy(nonEmpty)
	defer releaseContainerRun(emptyDiff)
	if emptyDiff == empty {
		t.Fatalf("andNotRunCopy must return a fresh run when the source is empty")
	}
	assertRunIntervalsInvariant(t, emptyDiff)
	assertSameContainerSet(t, emptyDiff, nil)

	xorLeft := nonEmpty.xorRun(empty)
	defer releaseContainer(xorLeft)
	assertSameContainerSet(t, xorLeft, []uint16{10, 11, 12, 100, 101})

	xorRight := empty.xorRun(nonEmpty)
	defer releaseContainer(xorRight)
	assertSameContainerSet(t, xorRight, []uint16{10, 11, 12, 100, 101})
}

func TestContainerRunXorRunDirectMergeRegressions(t *testing.T) {
	t.Run("OverlapAndAdjacency", func(t *testing.T) {
		leftIDs := append(containerRangeUint16(10, 13), containerRangeUint16(20, 23)...)
		leftIDs = append(leftIDs, containerRangeUint16(30, 36)...)
		rightIDs := append(containerRangeUint16(13, 20), containerRangeUint16(22, 25)...)
		rightIDs = append(rightIDs, containerRangeUint16(32, 41)...)
		want := append(containerRangeUint16(10, 22), containerRangeUint16(23, 25)...)
		want = append(want, containerRangeUint16(30, 32)...)
		want = append(want, containerRangeUint16(36, 41)...)

		left := buildContainerRun(leftIDs).(*containerRun)
		defer releaseContainerRun(left)
		right := buildContainerRun(rightIDs).(*containerRun)
		defer releaseContainerRun(right)

		result := left.xorRun(right)
		defer releaseContainer(result)

		assertSameContainerSet(t, result, want)
		assertRunIntervalsInvariant(t, left)
		assertRunIntervalsInvariant(t, right)
		assertSameContainerSet(t, left, leftIDs)
		assertSameContainerSet(t, right, rightIDs)
	})

	t.Run("TouchesMaxUint16", func(t *testing.T) {
		leftIDs := containerRangeUint16(maxCapacity-3, maxCapacity)
		rightIDs := containerRangeUint16(maxCapacity-2, maxCapacity)

		left := buildContainerRun(leftIDs).(*containerRun)
		defer releaseContainerRun(left)
		right := buildContainerRun(rightIDs).(*containerRun)
		defer releaseContainerRun(right)

		result := left.xorRun(right)
		defer releaseContainer(result)

		assertSameContainerSet(t, result, []uint16{uint16(maxCapacity - 3)})
		assertSameContainerSet(t, left, leftIDs)
		assertSameContainerSet(t, right, rightIDs)
	})
}

func TestContainerRunXorArrayDirectMergeRegressions(t *testing.T) {
	t.Run("BridgeWithSingletons", func(t *testing.T) {
		runIDs := append(containerRangeUint16(10, 13), containerRangeUint16(14, 17)...)
		arrayIDs := []uint16{9, 13, 17}
		want := containerRangeUint16(9, 18)

		run := buildContainerRun(runIDs).(*containerRun)
		defer releaseContainerRun(run)
		array := newContainerArrayFromSlice(arrayIDs)
		defer releaseContainerArray(array)

		result := run.xorArray(array)
		defer releaseContainer(result)

		assertSameContainerSet(t, result, want)
		assertRunIntervalsInvariant(t, run)
		assertSameContainerSet(t, run, runIDs)
		assertSameContainerSet(t, array, arrayIDs)
	})

	t.Run("SplitNearMaxUint16", func(t *testing.T) {
		runIDs := containerRangeUint16(maxCapacity-4, maxCapacity)
		arrayIDs := []uint16{uint16(maxCapacity - 2)}
		want := []uint16{uint16(maxCapacity - 4), uint16(maxCapacity - 3), uint16(maxCapacity - 1)}

		run := buildContainerRun(runIDs).(*containerRun)
		defer releaseContainerRun(run)
		array := newContainerArrayFromSlice(arrayIDs)
		defer releaseContainerArray(array)

		result := run.xorArray(array)
		defer releaseContainer(result)

		assertSameContainerSet(t, result, want)
		assertSameContainerSet(t, run, runIDs)
		assertSameContainerSet(t, array, arrayIDs)
	})
}

func TestContainerRunOrArrayMinimizationPaths(t *testing.T) {
	t.Run("Array", func(t *testing.T) {
		leftIDs := []uint16{0, 4, 8, 12, 16}
		rightIDs := []uint16{24, 28, 32}

		left := newContainerRunCopyIv(singletonIntervals16(leftIDs))
		defer releaseContainerRun(left)
		right := newContainerArrayFromSlice(rightIDs)
		defer releaseContainerArray(right)

		result := left.orArray(right)
		defer releaseContainer(result)

		if _, ok := result.(*containerArray); !ok {
			t.Fatalf("sparse run+array union must minimize to array, got %T", result)
		}
		assertSameContainerSet(t, result, unionUint16(leftIDs, rightIDs))
		assertRunIntervalsInvariant(t, left)
		assertSameContainerSet(t, left, leftIDs)
	})

	t.Run("Bitmap", func(t *testing.T) {
		leftIDs := make([]uint16, arrayDefaultMaxSize)
		for i := range leftIDs {
			leftIDs[i] = uint16(i * 2)
		}
		rightIDs := []uint16{uint16(arrayDefaultMaxSize * 2)}

		left := newContainerRunCopyIv(singletonIntervals16(leftIDs))
		defer releaseContainerRun(left)
		right := newContainerArrayFromSlice(rightIDs)
		defer releaseContainerArray(right)

		result := left.orArray(right)
		defer releaseContainer(result)

		if _, ok := result.(*containerBitmap); !ok {
			t.Fatalf("large sparse run+array union must minimize to bitmap, got %T", result)
		}
		assertSameContainerSet(t, result, unionUint16(leftIDs, rightIDs))
		assertRunIntervalsInvariant(t, left)
		assertSameContainerSet(t, left, leftIDs)
	})
}

func TestContainerRunIOrArrayDonorPathMinimization(t *testing.T) {
	t.Run("Array", func(t *testing.T) {
		leftIDs := []uint16{0, 4, 8, 12, 16}
		rightIDs := []uint16{24, 28, 32}

		left := newContainerRunCopyIv(singletonIntervals16(leftIDs))
		right := newContainerArrayFromSlice(rightIDs)
		defer releaseContainerArray(right)

		result := left.iorArray(right)
		if _, ok := result.(*containerArray); !ok {
			t.Fatalf("donor-path sparse run+array inplace union must minimize to array, got %T", result)
		}
		assertSameContainerSet(t, result, unionUint16(leftIDs, rightIDs))
		releaseContainerPair(left, result)
	})

	t.Run("Bitmap", func(t *testing.T) {
		leftIDs := make([]uint16, arrayDefaultMaxSize)
		for i := range leftIDs {
			leftIDs[i] = uint16(i * 2)
		}
		rightIDs := []uint16{uint16(arrayDefaultMaxSize * 2)}

		left := newContainerRunCopyIv(singletonIntervals16(leftIDs))
		right := newContainerArrayFromSlice(rightIDs)
		defer releaseContainerArray(right)

		result := left.iorArray(right)
		if _, ok := result.(*containerBitmap); !ok {
			t.Fatalf("donor-path large sparse run+array inplace union must minimize to bitmap, got %T", result)
		}
		assertSameContainerSet(t, result, unionUint16(leftIDs, rightIDs))
		releaseContainerPair(left, result)
	})
}

func TestContainerRunConversionsIteratorsAndClone(t *testing.T) {
	rc := buildContainerRun([]uint16{1, 2, 3, 10, 11, 12, 100, 101, 102}).(*containerRun)
	defer releaseContainerRun(rc)

	if got := rc.numberOfRuns(); got != 3 {
		t.Fatalf("numberOfRuns mismatch: got=%d want=3", got)
	}
	if got := rc.minimum(); got != 1 {
		t.Fatalf("minimum mismatch: got=%d want=1", got)
	}
	if got := rc.maximum(); got != 102 {
		t.Fatalf("maximum mismatch: got=%d want=102", got)
	}
	if got := rc.asSlice(); !slices.Equal(got, containerToSlice(rc)) {
		t.Fatalf("asSlice mismatch: got=%v want=%v", got, containerToSlice(rc))
	}

	bitmap := rc.toBitmapContainer()
	defer releaseContainerBitmap(bitmap)
	assertSameContainerSet(t, bitmap, containerToSlice(rc))

	array := rc.toArrayContainer()
	defer releaseContainerArray(array)
	assertSameContainerSet(t, array, containerToSlice(rc))

	optimized := rc.toEfficientContainer()
	defer releaseContainer(optimized)
	if _, ok := optimized.(*containerRun); !ok {
		t.Fatalf("expected run from toEfficientContainer for compact run cardinality, got %T", optimized)
	}

	optimizedFromCard := rc.toEfficientContainerFromCardinality(rc.getCardinality())
	defer releaseContainer(optimizedFromCard)
	if _, ok := optimizedFromCard.(*containerRun); !ok {
		t.Fatalf("expected run from toEfficientContainerFromCardinality, got %T", optimizedFromCard)
	}

	runIter := rc.newRunIterator()
	var iterIDs []uint16
	for runIter.hasNext() {
		iterIDs = append(iterIDs, runIter.next())
	}
	if !slices.Equal(iterIDs, containerToSlice(rc)) {
		t.Fatalf("run iterator mismatch: got=%v want=%v", iterIDs, containerToSlice(rc))
	}

	runIter = rc.newRunIterator()
	runIter.advanceIfNeeded(11)
	if got := runIter.peekNext(); got != 11 {
		t.Fatalf("advanceIfNeeded mismatch: got=%d want=11", got)
	}

	short := rc.getShortIterator()
	short.advanceIfNeeded(10)
	if got := short.peekNext(); got != 10 {
		t.Fatalf("getShortIterator advance mismatch: got=%d want=10", got)
	}

	runMany := rc.newRunManyIterator()
	buf := make([]uint32, 16)
	n := runMany.nextMany(1<<16, buf)
	wantMany := make([]uint32, len(containerToSlice(rc)))
	for i, v := range containerToSlice(rc) {
		wantMany[i] = 1<<16 | uint32(v)
	}
	if !slices.Equal(buf[:n], wantMany) {
		t.Fatalf("nextMany mismatch: got=%v want=%v", buf[:n], wantMany)
	}

	buf64 := make([]uint64, 16)
	runMany = rc.newRunManyIterator()
	n = runMany.nextMany64(2<<32, buf64)
	wantMany64 := make([]uint64, len(containerToSlice(rc)))
	for i, v := range containerToSlice(rc) {
		wantMany64[i] = 2<<32 | uint64(v)
	}
	if !slices.Equal(buf64[:n], wantMany64) {
		t.Fatalf("nextMany64 mismatch: got=%v want=%v", buf64[:n], wantMany64)
	}

	many := rc.getManyIterator()
	buf2 := make([]uint32, 16)
	n = many.nextMany(3<<16, buf2)
	wantMany2 := make([]uint32, len(containerToSlice(rc)))
	for i, v := range containerToSlice(rc) {
		wantMany2[i] = 3<<16 | uint32(v)
	}
	if !slices.Equal(buf2[:n], wantMany2) {
		t.Fatalf("getManyIterator mismatch: got=%v want=%v", buf2[:n], wantMany2)
	}

	fillBuf := make([]uint32, rc.getCardinality())
	n = rc.fillLeastSignificant16bits(fillBuf, 0, 3<<16)
	if got := fillBuf[:n]; got[0] != 3<<16|1 || got[len(got)-1] != 3<<16|102 {
		t.Fatalf("fillLeastSignificant16bits mismatch: got=%v", got)
	}

	cloneRun := rc.cloneRun()
	defer releaseContainerRun(cloneRun)
	cloneRun.iadd(400)
	if rc.contains(400) {
		t.Fatalf("cloneRun mutation changed source")
	}

	clone := rc.clone().(*containerRun)
	defer releaseContainerRun(clone)
	clone.iadd(500)
	if rc.contains(500) {
		t.Fatalf("clone mutation changed source")
	}

	runEq := buildContainerRun(containerToSlice(rc))
	defer releaseContainer(runEq)
	if !rc.equals(runEq) {
		t.Fatalf("run equals mismatch for same content")
	}
	arrayEq := buildContainerArray(containerToSlice(rc))
	defer releaseContainer(arrayEq)
	if !rc.equals(arrayEq) || !arrayEq.equals(rc) {
		t.Fatalf("run/array equals mismatch for same content")
	}
	bitmapEq := buildContainerBitmap(containerToSlice(rc))
	defer releaseContainer(bitmapEq)
	if !rc.equals(bitmapEq) || !bitmapEq.equals(rc) {
		t.Fatalf("run/bitmap equals mismatch for same content")
	}
	runDifferent := buildContainerArray([]uint16{1, 2, 3})
	defer releaseContainer(runDifferent)
	if rc.equals(runDifferent) {
		t.Fatalf("run equals must reject different content")
	}
}

func TestContainerRunToEfficientContainer_ArrayPathWithManySingletonRuns(t *testing.T) {
	values := make([]uint16, arrayDefaultMaxSize)
	for i := range values {
		values[i] = uint16(i * 2)
	}

	rc := buildContainerRun(values).(*containerRun)
	defer releaseContainerRun(rc)

	result := rc.toEfficientContainer()
	defer releaseContainer(result)

	array, ok := result.(*containerArray)
	if !ok {
		t.Fatalf("expected array container, got %T", result)
	}
	assertSameContainerSet(t, array, values)
}

func TestContainerRunToArrayContainer_TailAtMaxCapacity(t *testing.T) {
	rc := newContainerRunRange(maxCapacity-8, maxCapacity-1)
	defer releaseContainerRun(rc)

	array := rc.toArrayContainer()
	defer releaseContainerArray(array)

	assertSameContainerSet(t, array, containerRangeUint16(maxCapacity-8, maxCapacity))
}

func TestContainerRunToEfficientContainerFromCardinality_ArrayPath(t *testing.T) {
	values := make([]uint16, 128)
	for i := range values {
		values[i] = uint16(i * 2)
	}

	rc := buildContainerRun(values).(*containerRun)
	defer releaseContainerRun(rc)

	optimized := rc.toEfficientContainerFromCardinality(len(values))
	defer releaseContainer(optimized)

	array, ok := optimized.(*containerArray)
	if !ok {
		t.Fatalf("expected array from toEfficientContainerFromCardinality, got %T", optimized)
	}
	assertSameContainerSet(t, array, values)
}

func TestContainerRunDirectSubtractHelper(t *testing.T) {
	rc := buildContainerRun([]uint16{10, 11, 12, 20, 21, 22, 30, 31, 32}).(*containerRun)
	defer releaseContainerRun(rc)

	rc.isubtract(newInterval16Range(11, 30))
	assertRunIntervalsInvariant(t, rc)
	assertSameContainerSet(t, rc, []uint16{10, 31, 32})
}

func TestContainerRunDirectAddDeleteAndInvertHelpers(t *testing.T) {
	t.Run("AddBranches", func(t *testing.T) {
		tests := []struct {
			name string
			rc   *containerRun
			add  uint16
			want []uint16
		}{
			{name: "Empty", rc: newContainerRun(), add: 5, want: []uint16{5}},
			{name: "ExtendFirstBackward", rc: newContainerRunRange(5, 5), add: 4, want: []uint16{4, 5}},
			{name: "ExtendLastAtEnd", rc: newContainerRunRange(1, 2), add: 3, want: []uint16{1, 2, 3}},
			{name: "AppendAtEnd", rc: newContainerRunRange(1, 2), add: 5, want: []uint16{1, 2, 5}},
			{name: "FuseMiddleIntervals", rc: newContainerRunCopyIv([]interval16{newInterval16Range(1, 2), newInterval16Range(4, 5)}), add: 3, want: []uint16{1, 2, 3, 4, 5}},
			{name: "ExtendLeftInMiddle", rc: newContainerRunCopyIv([]interval16{newInterval16Range(1, 2), newInterval16Range(5, 6)}), add: 3, want: []uint16{1, 2, 3, 5, 6}},
			{name: "ExtendRightInMiddle", rc: newContainerRunCopyIv([]interval16{newInterval16Range(1, 1), newInterval16Range(5, 6)}), add: 4, want: []uint16{1, 4, 5, 6}},
			{name: "InsertStandaloneMiddle", rc: newContainerRunCopyIv([]interval16{newInterval16Range(1, 1), newInterval16Range(5, 6)}), add: 3, want: []uint16{1, 3, 5, 6}},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				defer releaseContainerRun(tc.rc)
				if !tc.rc.add(tc.add) {
					t.Fatalf("expected add(%d) to report insertion", tc.add)
				}
				assertRunIntervalsInvariant(t, tc.rc)
				assertSameContainerSet(t, tc.rc, tc.want)
			})
		}
	})

	t.Run("DeleteAtBranches", func(t *testing.T) {
		t.Run("DeleteSingleValueInterval", func(t *testing.T) {
			rc := newContainerRunRange(10, 10)
			defer releaseContainerRun(rc)
			curIndex, curPos := 0, uint16(0)
			rc.deleteAt(&curIndex, &curPos)
			if !rc.isEmpty() || curIndex != 0 || curPos != 0 {
				t.Fatalf("deleteAt single-value mismatch: rc=%v curIndex=%d curPos=%d", rc.iv, curIndex, curPos)
			}
		})

		t.Run("DeleteIntervalStart", func(t *testing.T) {
			rc := newContainerRunRange(20, 22)
			defer releaseContainerRun(rc)
			curIndex, curPos := 0, uint16(0)
			rc.deleteAt(&curIndex, &curPos)
			assertSameContainerSet(t, rc, []uint16{21, 22})
			if curIndex != 0 || curPos != 0 {
				t.Fatalf("deleteAt start cursor mismatch: curIndex=%d curPos=%d", curIndex, curPos)
			}
		})

		t.Run("DeleteIntervalEnd", func(t *testing.T) {
			rc := newContainerRunRange(30, 32)
			defer releaseContainerRun(rc)
			curIndex, curPos := 0, uint16(2)
			rc.deleteAt(&curIndex, &curPos)
			assertSameContainerSet(t, rc, []uint16{30, 31})
			if curIndex != 0 || curPos != 1 {
				t.Fatalf("deleteAt end cursor mismatch: curIndex=%d curPos=%d", curIndex, curPos)
			}
		})

		t.Run("DeleteIntervalMiddle", func(t *testing.T) {
			rc := newContainerRunRange(40, 44)
			defer releaseContainerRun(rc)
			curIndex, curPos := 0, uint16(2)
			rc.deleteAt(&curIndex, &curPos)
			assertRunIntervalsInvariant(t, rc)
			assertSameContainerSet(t, rc, []uint16{40, 41, 43, 44})
			if curIndex != 1 || curPos != 0 {
				t.Fatalf("deleteAt middle cursor mismatch: curIndex=%d curPos=%d", curIndex, curPos)
			}
		})
	})

	t.Run("IAddRangeBranches", func(t *testing.T) {
		tests := []struct {
			name  string
			rc    *containerRun
			start int
			endx  int
			want  []uint16
		}{
			{name: "Empty", rc: newContainerRun(), start: 5, endx: 8, want: []uint16{5, 6, 7}},
			{name: "InsertBeforeFirst", rc: newContainerRunCopyIv([]interval16{newInterval16Range(10, 12), newInterval16Range(20, 22)}), start: 1, endx: 4, want: []uint16{1, 2, 3, 10, 11, 12, 20, 21, 22}},
			{name: "MergeLeftOnly", rc: newContainerRunCopyIv([]interval16{newInterval16Range(10, 12), newInterval16Range(20, 22)}), start: 13, endx: 15, want: []uint16{10, 11, 12, 13, 14, 20, 21, 22}},
			{name: "MergeRightOnly", rc: newContainerRunCopyIv([]interval16{newInterval16Range(10, 12), newInterval16Range(20, 22)}), start: 17, endx: 20, want: []uint16{10, 11, 12, 17, 18, 19, 20, 21, 22}},
			{name: "BridgeMultipleIntervals", rc: newContainerRunCopyIv([]interval16{newInterval16Range(10, 12), newInterval16Range(15, 16), newInterval16Range(18, 20)}), start: 13, endx: 18, want: []uint16{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
			{name: "AppendAfterLast", rc: newContainerRunRange(10, 12), start: 20, endx: 23, want: []uint16{10, 11, 12, 20, 21, 22}},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				defer releaseContainerRun(tc.rc)
				tc.rc.iaddRange(tc.start, tc.endx)
				assertRunIntervalsInvariant(t, tc.rc)
				assertSameContainerSet(t, tc.rc, tc.want)
			})
		}
	})

	t.Run("RunArrayUnionReusedBuffer", func(t *testing.T) {
		ac := newContainerArrayFromSlice([]uint16{1, 3, 5, 40, 42, 44})
		defer releaseContainerArray(ac)

		src := make([]interval16, 2, 2+len(ac.content))
		src[0] = newInterval16Range(10, 20)
		src[1] = newInterval16Range(30, 35)

		buf := src[:cap(src)]
		copy(buf[len(ac.content):], src)

		got, cardMinusOne := runArrayUnionToRuns(buf[len(ac.content):], ac, buf[:0])
		rc := newContainerRunTakeOwnership(got)
		defer releaseContainerRun(rc)

		if int(cardMinusOne)+1 != rc.getCardinality() {
			t.Fatalf("runArrayUnionToRuns cardinality mismatch: got=%d want=%d", int(cardMinusOne)+1, rc.getCardinality())
		}
		assertRunIntervalsInvariant(t, rc)
		assertSameContainerSet(t, rc, []uint16{
			1, 3, 5,
			10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
			30, 31, 32, 33, 34, 35,
			40, 42, 44,
		})
	})

	t.Run("InvertLastIntervalBranches", func(t *testing.T) {
		tests := []struct {
			name   string
			rc     *containerRun
			origin uint16
			want   []interval16
		}{
			{name: "AllBitsCovered", rc: newContainerRunRange(0, MaxUint16), origin: 0, want: nil},
			{name: "PrefixOnly", rc: newContainerRunRange(10, MaxUint16), origin: 0, want: []interval16{newInterval16Range(0, 9)}},
			{name: "SuffixOnly", rc: newContainerRunRange(10, 20), origin: 10, want: []interval16{newInterval16Range(21, MaxUint16)}},
			{name: "Split", rc: newContainerRunRange(10, 20), origin: 0, want: []interval16{newInterval16Range(0, 9), newInterval16Range(21, MaxUint16)}},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				defer releaseContainerRun(tc.rc)
				if got := tc.rc.invertlastInterval(tc.origin, 0); !slices.Equal(got, tc.want) {
					t.Fatalf("invertlastInterval mismatch: got=%v want=%v", got, tc.want)
				}
			})
		}
	})
}

func TestContainerRunISubtractBranchMatrix(t *testing.T) {
	tests := []struct {
		name string
		rc   *containerRun
		del  interval16
		want []uint16
	}{
		{
			name: "EmptyNoop",
			rc:   newContainerRun(),
			del:  newInterval16Range(10, 20),
			want: nil,
		},
		{
			name: "OutsideHullNoop",
			rc:   newContainerRunCopyIv([]interval16{newInterval16Range(10, 12), newInterval16Range(20, 22)}),
			del:  newInterval16Range(30, 40),
			want: []uint16{10, 11, 12, 20, 21, 22},
		},
		{
			name: "ClearAllFromOutside",
			rc:   newContainerRunCopyIv([]interval16{newInterval16Range(10, 12), newInterval16Range(20, 22)}),
			del:  newInterval16Range(0, 30),
			want: nil,
		},
		{
			name: "BothPresentShrink",
			rc:   newContainerRunCopyIv([]interval16{newInterval16Range(10, 20), newInterval16Range(30, 40), newInterval16Range(50, 60)}),
			del:  newInterval16Range(15, 55),
			want: []uint16{10, 11, 12, 13, 14, 56, 57, 58, 59, 60},
		},
		{
			name: "BothPresentSameSize",
			rc:   newContainerRunCopyIv([]interval16{newInterval16Range(10, 20), newInterval16Range(30, 40)}),
			del:  newInterval16Range(15, 35),
			want: []uint16{10, 11, 12, 13, 14, 36, 37, 38, 39, 40},
		},
		{
			name: "BothPresentSplitSingleInterval",
			rc:   newContainerRunRange(10, 20),
			del:  newInterval16Range(13, 17),
			want: []uint16{10, 11, 12, 18, 19, 20},
		},
		{
			name: "NeitherPresentDropMiddle",
			rc:   newContainerRunCopyIv([]interval16{newInterval16Range(10, 20), newInterval16Range(30, 40), newInterval16Range(50, 60), newInterval16Range(70, 80)}),
			del:  newInterval16Range(25, 65),
			want: []uint16{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80},
		},
		{
			name: "StartPresentLastAbsent",
			rc:   newContainerRunCopyIv([]interval16{newInterval16Range(10, 20), newInterval16Range(30, 40), newInterval16Range(50, 60)}),
			del:  newInterval16Range(15, 45),
			want: []uint16{10, 11, 12, 13, 14, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60},
		},
		{
			name: "StartAbsentLastPresent",
			rc:   newContainerRunCopyIv([]interval16{newInterval16Range(10, 20), newInterval16Range(30, 40), newInterval16Range(50, 60)}),
			del:  newInterval16Range(25, 35),
			want: []uint16{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 36, 37, 38, 39, 40, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer releaseContainerRun(tc.rc)
			tc.rc.isubtract(tc.del)
			assertRunIntervalsInvariant(t, tc.rc)
			assertSameContainerSet(t, tc.rc, tc.want)
		})
	}
}

func TestContainerRunIntersectOutputCanExceedMinInputRuns(t *testing.T) {
	left := newContainerRunRange(0, 20)
	defer releaseContainerRun(left)

	right := newContainerRunCopyIv([]interval16{
		newInterval16Range(1, 2),
		newInterval16Range(5, 6),
		newInterval16Range(9, 10),
	})
	defer releaseContainerRun(right)

	got := left.intersect(right)
	defer releaseContainerRun(got)

	assertRunIntervalsInvariant(t, got)
	assertSameContainerSet(t, got, []uint16{1, 2, 5, 6, 9, 10})
	if len(got.iv) != 3 {
		t.Fatalf("intersect run count mismatch: got=%d want=3", len(got.iv))
	}
	if len(got.iv) <= minOfInt(len(left.iv), len(right.iv)) {
		t.Fatalf("intersect must be able to produce more than min input runs: left=%d right=%d got=%d", len(left.iv), len(right.iv), len(got.iv))
	}
}
