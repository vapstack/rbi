package posting

import (
	"slices"
	"testing"
)

func TestBitmapRangePrimitivesAndScanningHelpers(t *testing.T) {
	bitmap := make([]uint64, bitmapContainerWords)
	if delta := setBitmapRangeAndCardinalityChange(bitmap, 3, 8); delta != 5 {
		t.Fatalf("setBitmapRange delta mismatch: got=%d want=5", delta)
	}
	if delta := setBitmapRangeAndCardinalityChange(bitmap, 64, 130); delta != 66 {
		t.Fatalf("setBitmapRange multiword delta mismatch: got=%d want=66", delta)
	}
	if delta := flipBitmapRangeAndCardinalityChange(bitmap, 5, 10); delta != -1 {
		t.Fatalf("flipBitmapRange delta mismatch: got=%d want=-1", delta)
	}
	if delta := resetBitmapRangeAndCardinalityChange(bitmap, 64, 67); delta != -3 {
		t.Fatalf("resetBitmapRange delta mismatch: got=%d want=-3", delta)
	}

	bc := newContainerBitmap()
	defer bc.release()
	bc.bitmap = bitmap
	bc.computeCardinality()

	if got := bc.minimum(); got != 3 {
		t.Fatalf("minimum mismatch: got=%d want=3", got)
	}
	if got := bc.maximum(); got != 129 {
		t.Fatalf("maximum mismatch: got=%d want=129", got)
	}
	if got := bc.nextSetBit(0); got != 3 {
		t.Fatalf("nextSetBit mismatch: got=%d want=3", got)
	}
	if got := bc.nextUnsetBit(3); got != 5 {
		t.Fatalf("nextUnsetBit mismatch: got=%d want=5", got)
	}
	if got := bc.prevSetBit(129); got != 129 {
		t.Fatalf("prevSetBit mismatch: got=%d want=129", got)
	}
	if got := bc.uPrevSetBit(4); got != 4 {
		t.Fatalf("uPrevSetBit mismatch: got=%d want=4", got)
	}
	if got := bc.getCardinalityInRange(64, 130); got != 63 {
		t.Fatalf("getCardinalityInRange mismatch: got=%d want=63", got)
	}
}

func TestBitmapRangePrimitivesDirectBitOps(t *testing.T) {
	assertBitmapWords := func(t *testing.T, words []uint64, want []uint16) {
		t.Helper()
		bc := &containerBitmap{bitmap: words}
		bc.computeCardinality()
		assertSameContainerSet(t, bc, want)
	}

	t.Run("SetBitmapRange", func(t *testing.T) {
		words := make([]uint64, bitmapContainerWords)
		setBitmapRange(words, 9, 9)
		assertBitmapWords(t, words, nil)

		setBitmapRange(words, 3, 8)
		assertBitmapWords(t, words, []uint16{3, 4, 5, 6, 7})

		words = make([]uint64, bitmapContainerWords)
		setBitmapRange(words, 62, 130)
		assertBitmapWords(t, words, containerRangeUint16(62, 130))

		words = make([]uint64, bitmapContainerWords)
		setBitmapRange(words, 0, maxCapacity)
		bc := &containerBitmap{bitmap: words}
		bc.computeCardinality()
		if !bc.isFull() {
			t.Fatalf("full-domain setBitmapRange must produce full bitmap")
		}
	})

	t.Run("ResetBitmapRange", func(t *testing.T) {
		words := make([]uint64, bitmapContainerWords)
		setBitmapRange(words, 0, maxCapacity)
		resetBitmapRange(words, 5, 5)
		bc := &containerBitmap{bitmap: words}
		bc.computeCardinality()
		if !bc.isFull() {
			t.Fatalf("empty resetBitmapRange must keep full bitmap unchanged")
		}

		resetBitmapRange(words, 3, 8)
		assertBitmapWords(t, words, differenceUint16(containerRangeUint16(0, maxCapacity), containerRangeUint16(3, 8)))

		words = make([]uint64, bitmapContainerWords)
		setBitmapRange(words, 0, maxCapacity)
		resetBitmapRange(words, 64, 130)
		assertBitmapWords(t, words, differenceUint16(containerRangeUint16(0, maxCapacity), containerRangeUint16(64, 130)))

		resetBitmapRange(words, 0, maxCapacity)
		assertBitmapWords(t, words, nil)
	})

	t.Run("FlipBitmapRange", func(t *testing.T) {
		words := make([]uint64, bitmapContainerWords)
		flipBitmapRange(words, 7, 7)
		assertBitmapWords(t, words, nil)

		flipBitmapRange(words, 3, 8)
		assertBitmapWords(t, words, []uint16{3, 4, 5, 6, 7})

		flipBitmapRange(words, 5, 10)
		assertBitmapWords(t, words, []uint16{3, 4, 8, 9})

		words = make([]uint64, bitmapContainerWords)
		flipBitmapRange(words, 62, 130)
		assertBitmapWords(t, words, containerRangeUint16(62, 130))

		flipBitmapRange(words, 0, maxCapacity)
		assertBitmapWords(t, words, differenceUint16(containerRangeUint16(0, maxCapacity), containerRangeUint16(62, 130)))
	})
}

func TestContainerBitmapCrossTypeOps(t *testing.T) {
	leftIDs := []uint16{1, 3, 5, 7, 9, 11, 30, 31}
	rightIDs := []uint16{3, 4, 7, 8, 10, 11, 12, 30, 32}

	for _, factory := range containerFactories() {
		t.Run(factory.name, func(t *testing.T) {
			left := buildContainerBitmap(leftIDs)
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

			iorLeft := buildContainerBitmap(leftIDs)
			iorRight := factory.build(rightIDs)
			iorResult := iorLeft.ior(iorRight)
			assertSameContainerSet(t, iorResult, unionWant)
			iorRight.release()
			cleanupContainerPair(iorLeft, iorResult)

			iandLeft := buildContainerBitmap(leftIDs)
			iandRight := factory.build(rightIDs)
			iandResult := iandLeft.iand(iandRight)
			assertSameContainerSet(t, iandResult, andWant)
			iandRight.release()
			cleanupContainerPair(iandLeft, iandResult)

			iandNotLeft := buildContainerBitmap(leftIDs)
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
		})
	}
}

func TestContainerBitmapIAndNotRunSparseWideRanges(t *testing.T) {
	bc := newContainerBitmap()
	bc.iaddRange(0, 6000)

	removedIDs := []uint16{
		3, 4, 5,
		9, 10, 11,
		512, 513, 514, 515,
		2048, 2049, 2050,
		4090, 4091, 4092, 4093, 4094,
		5997, 5998, 5999,
	}
	rc := buildContainerRun(removedIDs).(*containerRun)
	defer rc.release()

	result := bc.iandNotRun(rc)
	defer cleanupContainerPair(bc, result)

	bitmapResult, ok := result.(*containerBitmap)
	if !ok {
		t.Fatalf("sparse wide-range iandNotRun should stay bitmap, got %T", result)
	}
	if bitmapResult != bc {
		t.Fatalf("iandNotRun should mutate bitmap receiver in place, got %p want %p", bitmapResult, bc)
	}

	assertSameContainerSet(t, bitmapResult, differenceUint16(containerRangeUint16(0, 6000), removedIDs))
}

func TestContainerBitmapConversionsIterationAndLifecycle(t *testing.T) {
	bc := newContainerBitmap()
	defer bc.release()
	bc.iaddRange(10, 20)
	bc.iaddRange(40, 45)

	ac := bc.toArrayContainer()
	defer ac.release()
	assertSameContainerSet(t, ac, []uint16{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 40, 41, 42, 43, 44})

	rc := newContainerRunRange(100, 120)
	defer rc.release()
	fromRun := newContainerBitmapFromRun(rc)
	defer fromRun.release()
	assertSameContainerSet(t, fromRun, []uint16{
		100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
		110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
	})

	runFriendly := newContainerBitmap()
	defer runFriendly.release()
	runFriendly.iaddRange(1, 128)
	optimized := runFriendly.toEfficientContainer()
	defer optimized.release()
	if _, ok := optimized.(*containerRun); !ok {
		t.Fatalf("expected run container from run-friendly bitmap, got %T", optimized)
	}

	var iterated []uint16
	if ok := bc.iterate(func(x uint16) bool {
		iterated = append(iterated, x)
		return true
	}); !ok {
		t.Fatalf("iterate unexpectedly returned false")
	}
	iteratedContainer := buildContainerArray(iterated)
	assertSameContainerSet(t, iteratedContainer, containerToSlice(ac))
	iteratedContainer.release()

	short := bc.getShortIterator()
	var shortIDs []uint16
	for short.hasNext() {
		shortIDs = append(shortIDs, short.next())
	}
	if !slices.Equal(shortIDs, containerToSlice(ac)) {
		t.Fatalf("short iterator mismatch: got=%v want=%v", shortIDs, containerToSlice(ac))
	}

	many := bc.getManyIterator()
	buf := make([]uint32, 32)
	n := many.nextMany(1<<16, buf)
	wantMany := make([]uint32, len(containerToSlice(ac)))
	for i, v := range containerToSlice(ac) {
		wantMany[i] = 1<<16 | uint32(v)
	}
	if !slices.Equal(buf[:n], wantMany) {
		t.Fatalf("many iterator mismatch: got=%v want=%v", buf[:n], wantMany)
	}

	fillBuf := make([]uint16, bc.getCardinality())
	bc.fillArray(fillBuf)
	if !slices.Equal(fillBuf, containerToSlice(ac)) {
		t.Fatalf("fillArray mismatch: got=%v want=%v", fillBuf, containerToSlice(ac))
	}

	fillLSB := make([]uint32, bc.getCardinality())
	n = bc.fillLeastSignificant16bits(fillLSB, 0, 2<<16)
	if got := fillLSB[:n]; len(got) != bc.getCardinality() || got[0] != 2<<16|10 {
		t.Fatalf("fillLeastSignificant16bits mismatch: got=%v", got)
	}

	temp := newContainerBitmap()
	temp.iaddRange(5, 15)
	result := efficientContainerFromTempBitmap(temp)
	defer result.release()
	if _, ok := result.(*containerRun); !ok {
		t.Fatalf("expected run container from efficientContainerFromTempBitmap, got %T", result)
	}
}

func TestContainerBitmapBitmapRunFillHelpers(t *testing.T) {
	src := make([]uint64, bitmapContainerWords)
	setBitmapRange(src, 2, 6)
	setBitmapRange(src, 64, 69)
	setBitmapRange(src, 127, 131)
	setBitmapRange(src, 200, 206)

	iv := []interval16{
		newInterval16Range(0, 10),
		newInterval16Range(64, 130),
		newInterval16Range(198, 206),
	}

	dst := make([]uint64, bitmapContainerWords)
	fillBitmapAndRun(dst, src, iv)

	bc := &containerBitmap{bitmap: dst}
	bc.computeCardinality()
	assertSameContainerSet(t, bc, []uint16{
		2, 3, 4, 5,
		64, 65, 66, 67, 68,
		127, 128, 129, 130,
		200, 201, 202, 203, 204, 205,
	})

	arrayOut := make([]uint16, bc.getCardinality())
	n := fillArrayBitmapAndRun(arrayOut, src, iv)
	if !slices.Equal(arrayOut[:n], containerToSlice(bc)) {
		t.Fatalf("fillArrayBitmapAndRun mismatch: got=%v want=%v", arrayOut[:n], containerToSlice(bc))
	}
}

func TestContainerBitmapPoolReuseStartsClean(t *testing.T) {
	bc := newContainerBitmap()
	bc.iaddRange(10, 200)
	bc.iaddRange(500, 900)
	bc.release()

	reused := newContainerBitmap()
	defer reused.release()
	if !testRaceEnabled && reused != bc {
		t.Fatalf("bitmap container pool did not reuse instance")
	}
	if reused.cardinality != 0 {
		t.Fatalf("reused bitmap container leaked cardinality: %d", reused.cardinality)
	}
	if reused.nextSetBit(0) != -1 {
		t.Fatalf("reused bitmap container leaked bits")
	}
}

func TestContainerBitmapMutationHelpersAndEquality(t *testing.T) {
	bc := newContainerBitmap()
	defer bc.release()
	bc.iaddRange(0, arrayDefaultMaxSize+1)

	result := bc.iremoveReturnMinimized(arrayDefaultMaxSize)
	if _, ok := result.(*containerArray); !ok {
		t.Fatalf("iremoveReturnMinimized should shrink to array at threshold, got %T", result)
	}
	assertSameContainerSet(t, result, func() []uint16 {
		out := make([]uint16, 0, arrayDefaultMaxSize)
		for i := 0; i < arrayDefaultMaxSize; i++ {
			out = append(out, uint16(i))
		}
		return out
	}())
	result.release()

	bc2 := newContainerBitmap()
	defer bc2.release()
	bc2.iaddRange(0, 5000)
	if !bc2.iremove(7) || bc2.iremove(7) {
		t.Fatalf("iremove mismatch")
	}
	if bc2.getSizeInBytes() != bitmapContainerWords*8 {
		t.Fatalf("getSizeInBytes mismatch: got=%d want=%d", bc2.getSizeInBytes(), bitmapContainerWords*8)
	}

	trimmed := bc2.iremoveRange(4090, 5000)
	if _, ok := trimmed.(*containerArray); !ok {
		t.Fatalf("iremoveRange should shrink to array, got %T", trimmed)
	}
	trimmed.release()

	full := newContainerBitmap()
	defer full.release()
	full.iaddRange(0, maxCapacity)
	if !full.isFull() {
		t.Fatalf("bitmap must be full after covering whole range")
	}

	flipped := full.inot(0, maxCapacity)
	if _, ok := flipped.(*containerArray); !ok {
		t.Fatalf("full inot should become empty array, got %T", flipped)
	}
	if !flipped.isEmpty() {
		t.Fatalf("full inot should become empty")
	}
	flipped.release()

	source := newContainerBitmap()
	defer source.release()
	source.iaddRange(0, 5000)
	notResult := source.not(1000, 2000)
	defer notResult.release()
	if !source.contains(1500) {
		t.Fatalf("not must not mutate receiver")
	}
	if notResult.contains(1500) {
		t.Fatalf("not result must flip specified range")
	}

	short := source.getShortIterator()
	short.advanceIfNeeded(2048)
	if got := short.peekNext(); got != 2048 {
		t.Fatalf("bitmap short iterator advance mismatch: got=%d want=2048", got)
	}

	clone := source.clone().(*containerBitmap)
	defer clone.release()
	if !source.equals(clone) {
		t.Fatalf("bitmap equals mismatch for clone")
	}
	clone.iremove(1)
	if source.equals(clone) {
		t.Fatalf("bitmap equals must detect differences")
	}
	if !bitmapEquals(source.bitmap, source.bitmap) || bitmapEquals(source.bitmap[:1], source.bitmap[:2]) {
		t.Fatalf("bitmapEquals mismatch")
	}
	if got := source.nextUnsetBit(maxCapacity); got != maxCapacity {
		t.Fatalf("nextUnsetBit out-of-range mismatch: got=%d want=%d", got, maxCapacity)
	}
	if got := source.uPrevSetBit(uint(maxCapacity)); got != -1 {
		t.Fatalf("uPrevSetBit out-of-range mismatch: got=%d want=-1", got)
	}

	runLike := buildContainerRun([]uint16{0, 1, 2, 3, 4, 5})
	defer runLike.release()
	eqClone := source.clone()
	defer eqClone.release()
	if !source.equals(eqClone) {
		t.Fatalf("bitmap equals must accept same-type clone")
	}
	if source.equals(runLike) {
		t.Fatalf("bitmap equals must reject different content")
	}
}

func TestContainerBitmapBitmapSpecificBranches(t *testing.T) {
	fullLeft := newContainerBitmap()
	defer fullLeft.release()
	fullLeft.iaddRange(0, maxCapacity/2)

	fullRight := newContainerBitmap()
	defer fullRight.release()
	fullRight.iaddRange(maxCapacity/2, maxCapacity)

	fullXor := fullLeft.xorBitmap(fullRight)
	defer fullXor.release()
	if _, ok := fullXor.(*containerRun); !ok {
		t.Fatalf("xorBitmap must collapse full bitmap to run, got %T", fullXor)
	}
	if !fullXor.isFull() || !fullXor.contains(0) || !fullXor.contains(MaxUint16) {
		t.Fatalf("xorBitmap full-result mismatch")
	}

	denseLeft := newContainerBitmap()
	defer denseLeft.release()
	denseLeft.iaddRange(0, 6000)

	denseRight := newContainerBitmap()
	defer denseRight.release()
	denseRight.iaddRange(1000, 2000)

	denseDiff := denseLeft.andNotBitmap(denseRight)
	defer denseDiff.release()
	if _, ok := denseDiff.(*containerBitmap); !ok {
		t.Fatalf("andNotBitmap must stay bitmap for dense result, got %T", denseDiff)
	}
	if !denseDiff.contains(999) || denseDiff.contains(1500) || !denseDiff.contains(2000) {
		t.Fatalf("andNotBitmap dense-result mismatch")
	}

	sparseRight := newContainerBitmap()
	defer sparseRight.release()
	sparseRight.iaddRange(1, 6000)

	sparseDiff := denseLeft.andNotBitmap(sparseRight)
	defer sparseDiff.release()
	if _, ok := sparseDiff.(*containerArray); !ok {
		t.Fatalf("andNotBitmap must shrink to array for sparse result, got %T", sparseDiff)
	}
	assertSameContainerSet(t, sparseDiff, []uint16{0})

	scan := newContainerBitmap()
	defer scan.release()
	scan.iaddRange(0, 64)
	scan.iadd(130)
	if got := scan.nextUnsetBit(0); got != 64 {
		t.Fatalf("nextUnsetBit cross-word mismatch: got=%d want=64", got)
	}
	boundaryDense := newContainerBitmap()
	defer boundaryDense.release()
	boundaryDense.iaddRange(1, 65)
	if got := boundaryDense.nextUnsetBit(1); got != 65 {
		t.Fatalf("nextUnsetBit full-suffix boundary mismatch: got=%d want=65", got)
	}
	if got := scan.uPrevSetBit(129); got != 63 {
		t.Fatalf("uPrevSetBit same-word mismatch: got=%d want=63", got)
	}
	if got := scan.uPrevSetBit(128); got != 63 {
		t.Fatalf("uPrevSetBit cross-word mismatch: got=%d want=63", got)
	}
}

func TestContainerBitmapAndBitmapAndEqualsBranches(t *testing.T) {
	denseLeft := newContainerBitmap()
	defer denseLeft.release()
	denseLeft.iaddRange(0, 6000)

	denseRight := newContainerBitmap()
	defer denseRight.release()
	denseRight.iaddRange(1000, 7000)

	denseAnd := denseLeft.andBitmap(denseRight)
	defer denseAnd.release()
	if _, ok := denseAnd.(*containerBitmap); !ok {
		t.Fatalf("andBitmap must stay bitmap for dense result, got %T", denseAnd)
	}
	if !denseAnd.contains(1000) || denseAnd.contains(999) || !denseAnd.contains(5999) {
		t.Fatalf("andBitmap dense-result mismatch")
	}

	sparseRight := newContainerBitmap()
	defer sparseRight.release()
	sparseRight.iaddRange(0, 5)

	sparseAnd := denseLeft.andBitmap(sparseRight)
	defer sparseAnd.release()
	if _, ok := sparseAnd.(*containerArray); !ok {
		t.Fatalf("andBitmap must shrink to array for sparse result, got %T", sparseAnd)
	}
	assertSameContainerSet(t, sparseAnd, []uint16{0, 1, 2, 3, 4})

	self := denseLeft.equals(denseLeft)
	if !self {
		t.Fatalf("bitmap equals must accept same object")
	}

	sameArray := buildContainerArray([]uint16{0, 1, 2, 3, 4})
	defer sameArray.release()
	if !sparseRight.equals(sameArray) {
		t.Fatalf("bitmap equals must accept same content across container types")
	}

	diffArray := buildContainerArray([]uint16{0, 1, 2, 3})
	defer diffArray.release()
	if sparseRight.equals(diffArray) {
		t.Fatalf("bitmap equals must reject different cardinality")
	}
}

func TestContainerBitmapBitmapInPlaceSelfAlias(t *testing.T) {
	union := newContainerBitmap()
	defer union.release()
	union.iaddRange(0, 6000)

	unionResult := union.iorBitmap(union)
	if unionResult != union {
		t.Fatalf("iorBitmap self-alias must keep receiver, got %T", unionResult)
	}
	if union.getCardinality() != 6000 || !union.contains(0) || !union.contains(5999) {
		t.Fatalf("iorBitmap self-alias changed content")
	}

	andSelf := newContainerBitmap()
	defer andSelf.release()
	andSelf.iaddRange(0, 6000)

	andResult := andSelf.iandBitmap(andSelf)
	if andResult != andSelf {
		t.Fatalf("iandBitmap self-alias must keep dense bitmap receiver, got %T", andResult)
	}
	if andSelf.getCardinality() != 6000 || !andSelf.contains(0) || !andSelf.contains(5999) {
		t.Fatalf("iandBitmap self-alias changed content")
	}

	diffSelf := newContainerBitmap()
	diffSelf.iaddRange(0, 6000)

	diffResult := diffSelf.iandNot(diffSelf)
	defer cleanupContainerPair(diffSelf, diffResult)
	if _, ok := diffResult.(*containerArray); !ok {
		t.Fatalf("iandNot self-alias must shrink to array, got %T", diffResult)
	}
	assertSameContainerSet(t, diffResult, nil)
}
