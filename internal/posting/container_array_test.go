package posting

import (
	"slices"
	"testing"
)

func TestContainerArraySingleAndRangeOps(t *testing.T) {
	ac := getContainerArrayFromSlice([]uint16{3, 7, 11})
	defer ac.release()

	if !ac.iadd(5) || ac.iadd(5) {
		t.Fatalf("iadd duplicate detection mismatch")
	}
	if !ac.contains(5) || ac.contains(6) {
		t.Fatalf("contains mismatch")
	}
	if got := ac.minimum(); got != 3 {
		t.Fatalf("minimum mismatch: got=%d want=3", got)
	}
	if got := ac.maximum(); got != 11 {
		t.Fatalf("maximum mismatch: got=%d want=11", got)
	}

	ac.iaddRange(8, 10)
	assertSameContainerSet(t, ac, []uint16{3, 5, 7, 8, 9, 11})

	ac.iremoveRange(7, 10)
	assertSameContainerSet(t, ac, []uint16{3, 5, 11})

	flipped := ac.not(4, 7)
	defer flipped.release()
	assertSameContainerSet(t, flipped, []uint16{3, 4, 6, 11})

	inverted := ac.inot(0, 4)
	defer cleanupContainerPair(ac, inverted)
	ac = nil
	assertSameContainerSet(t, inverted, []uint16{0, 1, 2, 5, 11})
}

func TestContainerArrayCrossTypeOps(t *testing.T) {
	leftIDs := []uint16{1, 3, 5, 7, 9, 11, 30, 31}
	rightIDs := []uint16{3, 4, 7, 8, 10, 11, 12, 30, 32}

	for _, factory := range containerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			left := buildContainerArray(leftIDs)
			right := factory.build(rightIDs)
			defer left.release()
			defer right.release()

			unionWant := unionUint16(leftIDs, rightIDs)
			andWant := intersectUint16(leftIDs, rightIDs)
			xorWant := xorUint16(leftIDs, rightIDs)
			diffWant := differenceUint16(leftIDs, rightIDs)

			orResult := left.or(right)
			assertSameContainerSet(t, orResult, unionWant)
			orResult.release()

			andResult := left.and(right)
			assertSameContainerSet(t, andResult, andWant)
			andResult.release()

			xorResult := left.xor(right)
			assertSameContainerSet(t, xorResult, xorWant)
			xorResult.release()

			andNotResult := left.andNot(right)
			assertSameContainerSet(t, andNotResult, diffWant)
			andNotResult.release()

			iorLeft := buildContainerArray(leftIDs)
			iorRight := factory.build(rightIDs)
			iorResult := iorLeft.ior(iorRight)
			assertSameContainerSet(t, iorResult, unionWant)
			iorRight.release()
			cleanupContainerPair(iorLeft, iorResult)

			iandLeft := buildContainerArray(leftIDs)
			iandRight := factory.build(rightIDs)
			iandResult := iandLeft.iand(iandRight)
			assertSameContainerSet(t, iandResult, andWant)
			iandRight.release()
			cleanupContainerPair(iandLeft, iandResult)

			iandNotLeft := buildContainerArray(leftIDs)
			iandNotRight := factory.build(rightIDs)
			iandNotResult := iandNotLeft.iandNot(iandNotRight)
			assertSameContainerSet(t, iandNotResult, diffWant)
			iandNotRight.release()
			cleanupContainerPair(iandNotLeft, iandNotResult)

			if got := left.intersects(right); got != (len(andWant) > 0) {
				t.Fatalf("intersects mismatch: got=%v want=%v", got, len(andWant) > 0)
			}
			if got := left.andCardinality(right); got != len(andWant) {
				t.Fatalf("andCardinality mismatch: got=%d want=%d", got, len(andWant))
			}
			eq := buildContainerArray(leftIDs)
			if !left.equals(eq) {
				eq.release()
				t.Fatalf("equals mismatch for array clone")
			}
			eq.release()
		})
	}
}

func TestContainerArrayHelpersAndTransitions(t *testing.T) {
	ac := getContainerArrayFromSlice([]uint16{2, 4, 6, 8})
	defer ac.release()

	if ac.isFull() {
		t.Fatalf("array container must never report full")
	}

	var iterated []uint16
	if ok := ac.iterate(func(x uint16) bool {
		iterated = append(iterated, x)
		return true
	}); !ok {
		t.Fatalf("iterate unexpectedly returned false")
	}
	if !slices.Equal(iterated, []uint16{2, 4, 6, 8}) {
		t.Fatalf("iterate mismatch: got=%v", iterated)
	}

	short := ac.getShortIterator()
	var shortIDs []uint16
	for short.hasNext() {
		shortIDs = append(shortIDs, short.next())
	}
	if !slices.Equal(shortIDs, []uint16{2, 4, 6, 8}) {
		t.Fatalf("short iterator mismatch: got=%v", shortIDs)
	}

	many := ac.getManyIterator()
	buf := make([]uint32, 8)
	n := many.nextMany(1<<16, buf)
	if got := buf[:n]; !slices.Equal(got, []uint32{1<<16 | 2, 1<<16 | 4, 1<<16 | 6, 1<<16 | 8}) {
		t.Fatalf("many iterator mismatch: got=%v", got)
	}

	fillBuf := make([]uint32, 8)
	n = ac.fillLeastSignificant16bits(fillBuf, 0, 2<<16)
	if got := fillBuf[:n]; !slices.Equal(got, []uint32{2<<16 | 2, 2<<16 | 4, 2<<16 | 6, 2<<16 | 8}) {
		t.Fatalf("fillLeastSignificant16bits mismatch: got=%v", got)
	}

	if got := ac.numberOfRuns(); got != 4 {
		t.Fatalf("numberOfRuns mismatch: got=%d want=4", got)
	}

	bc := newContainerBitmap()
	bc.iaddRange(100, 110)
	defer bc.release()
	fromBitmap := newContainerArrayFromBitmap(bc)
	defer fromBitmap.release()
	assertSameContainerSet(t, fromBitmap, []uint16{100, 101, 102, 103, 104, 105, 106, 107, 108, 109})

	large := getContainerArrayWithLen(arrayDefaultMaxSize)
	for i := range large.content {
		large.content[i] = uint16(i * 2)
	}
	spilled := large.iaddReturnMinimized(uint16(arrayDefaultMaxSize*2 + 1))
	defer cleanupContainerPair(large, spilled)
	if _, ok := spilled.(*containerBitmap); !ok {
		t.Fatalf("expected bitmap spill after exceeding arrayDefaultMaxSize, got %T", spilled)
	}

	flipDense := getContainerArrayWithLen(arrayDefaultMaxSize)
	for i := range flipDense.content {
		flipDense.content[i] = uint16(i * 2)
	}
	flippedDense := flipDense.inot(1, arrayDefaultMaxSize*2)
	defer cleanupContainerPair(flipDense, flippedDense)
	if _, ok := flippedDense.(*containerBitmap); !ok {
		t.Fatalf("expected bitmap spill after inot promotion, got %T", flippedDense)
	}
	if flippedDense.getCardinality() != arrayDefaultMaxSize+1 {
		t.Fatalf("inot promotion cardinality mismatch: got=%d want=%d", flippedDense.getCardinality(), arrayDefaultMaxSize+1)
	}
	if !flippedDense.contains(0) || !flippedDense.contains(1) || !flippedDense.contains(uint16(arrayDefaultMaxSize*2-1)) {
		t.Fatalf("inot promotion lost expected edge values")
	}
	if flippedDense.contains(2) || flippedDense.contains(uint16(arrayDefaultMaxSize*2-2)) {
		t.Fatalf("inot promotion kept values that should have been flipped out")
	}

	runFriendly := getContainerArrayFromSlice([]uint16{10, 11, 12, 13, 14, 40, 41, 42, 43})
	defer runFriendly.release()
	optimized := runFriendly.toEfficientContainer()
	defer optimized.release()
	if _, ok := optimized.(*containerRun); !ok {
		t.Fatalf("expected run container from run-friendly array, got %T", optimized)
	}

	bitmapRight := newContainerBitmap()
	bitmapRight.iaddRange(1000, 6000)
	defer bitmapRight.release()
	bitmapUnion := getContainerArrayFromSlice([]uint16{1, 3, 5})
	iorBitmap := bitmapUnion.iorBitmap(bitmapRight)
	defer cleanupContainerPair(bitmapUnion, iorBitmap)
	if _, ok := iorBitmap.(*containerBitmap); !ok {
		t.Fatalf("iorBitmap must keep bitmap representation for dense union, got %T", iorBitmap)
	}
	if !iorBitmap.contains(1) || !iorBitmap.contains(5999) {
		t.Fatalf("iorBitmap lost union elements")
	}

	smallLeft := getContainerArrayFromSlice([]uint16{1, 3, 5})
	defer smallLeft.release()
	runRight := newContainerRunRange(10, 12)
	defer runRight.release()
	iorRun := smallLeft.iorRun(runRight)
	defer iorRun.release()
	assertSameContainerSet(t, iorRun, []uint16{1, 3, 5, 10, 11, 12})

	orA := getContainerArrayFromSlice([]uint16{1, 3, 5})
	defer orA.release()
	orB := getContainerArrayFromSlice([]uint16{2, 4, 6})
	defer orB.release()
	orArray := orA.orArray(orB)
	defer orArray.release()
	assertSameContainerSet(t, orArray, []uint16{1, 2, 3, 4, 5, 6})

	xorArray := orA.xorArray(orB)
	defer xorArray.release()
	assertSameContainerSet(t, xorArray, []uint16{1, 2, 3, 4, 5, 6})

	inserted := getContainerArrayFromSlice([]uint16{10, 20, 30})
	defer inserted.release()
	inserted.insertValue(1, 15)
	assertSameContainerSet(t, inserted, []uint16{10, 15, 20, 30})

	ac.realloc(16)
	if len(ac.content) != 16 {
		t.Fatalf("realloc length mismatch: got=%d want=16", len(ac.content))
	}
}

func TestContainerArrayDirectBranchCoverage(t *testing.T) {
	t.Run("DenseOrAndXorArrayBranches", func(t *testing.T) {
		leftDense := getContainerArrayWithLen(arrayDefaultMaxSize)
		defer leftDense.release()
		rightDense := getContainerArrayWithLen(arrayDefaultMaxSize)
		defer rightDense.release()
		for i := range leftDense.content {
			leftDense.content[i] = uint16(i * 2)
			rightDense.content[i] = uint16(i*2 + 1)
		}

		orDense := leftDense.orArray(rightDense)
		defer orDense.release()
		if _, ok := orDense.(*containerBitmap); !ok {
			t.Fatalf("dense orArray must spill to bitmap, got %T", orDense)
		}
		if !orDense.contains(0) || !orDense.contains(uint16(arrayDefaultMaxSize*2-1)) {
			t.Fatalf("dense orArray lost edge values")
		}

		xorDense := leftDense.xorArray(rightDense)
		defer xorDense.release()
		if _, ok := xorDense.(*containerBitmap); !ok {
			t.Fatalf("dense xorArray must spill to bitmap, got %T", xorDense)
		}
		if !xorDense.contains(1) || !xorDense.contains(uint16(arrayDefaultMaxSize*2-2)) {
			t.Fatalf("dense xorArray lost edge values")
		}

		sameLeft := getContainerArrayWithLen(arrayDefaultMaxSize)
		defer sameLeft.release()
		sameRight := getContainerArrayWithLen(arrayDefaultMaxSize)
		defer sameRight.release()
		for i := range sameLeft.content {
			sameLeft.content[i] = uint16(i * 2)
			sameRight.content[i] = uint16(i * 2)
		}

		orSame := sameLeft.orArray(sameRight)
		defer orSame.release()
		if _, ok := orSame.(*containerArray); !ok {
			t.Fatalf("dense duplicate orArray must shrink back to array, got %T", orSame)
		}
		assertSameContainerSet(t, orSame, containerToSlice(sameLeft))

		xorSame := sameLeft.xorArray(sameRight)
		defer xorSame.release()
		if _, ok := xorSame.(*containerArray); !ok {
			t.Fatalf("dense duplicate xorArray must shrink back to array, got %T", xorSame)
		}
		if !xorSame.isEmpty() {
			t.Fatalf("dense duplicate xorArray must be empty")
		}
	})

	t.Run("IorRunHeuristicAndFallback", func(t *testing.T) {
		heuristic := getContainerArrayFromSlice([]uint16{10, 20, 30, 40, 50, 60})
		smallRun := newContainerRunRange(25, 26)
		defer smallRun.release()
		heuristicResult := heuristic.iorRun(smallRun)
		defer cleanupContainerPair(heuristic, heuristicResult)
		assertSameContainerSet(t, heuristicResult, []uint16{10, 20, 25, 26, 30, 40, 50, 60})

		fallback := getContainerArrayFromSlice([]uint16{10, 20})
		defer fallback.release()
		largeRun := newContainerRunRange(100, 5000)
		defer largeRun.release()
		fallbackResult := fallback.iorRun(largeRun)
		defer fallbackResult.release()
		if _, ok := fallbackResult.(*containerRun); !ok {
			t.Fatalf("iorRun fallback must preserve run-backed union, got %T", fallbackResult)
		}
		if !fallbackResult.contains(10) || !fallbackResult.contains(5000) {
			t.Fatalf("iorRun fallback lost edge values")
		}
	})

	t.Run("EqualsAndInsertValueInPlace", func(t *testing.T) {
		inPlace := getContainerArrayWithCap(4)
		defer inPlace.release()
		inPlace.content = append(inPlace.content, 10, 20, 30)
		inPlace.insertValue(1, 15)
		assertSameContainerSet(t, inPlace, []uint16{10, 15, 20, 30})

		if !inPlace.equals(inPlace) {
			t.Fatalf("array equals must accept same object")
		}

		sameBitmap := buildContainerBitmap([]uint16{10, 15, 20, 30})
		defer sameBitmap.release()
		if !inPlace.equals(sameBitmap) {
			t.Fatalf("array equals must accept same content across container type")
		}

		diffBitmap := buildContainerBitmap([]uint16{10, 15, 20})
		defer diffBitmap.release()
		if inPlace.equals(diffBitmap) {
			t.Fatalf("array equals must reject different cardinality")
		}
	})
}
