package posting

import (
	"slices"
	"testing"
)

func ownContainerArray(ac *containerArray) *containerArray {
	if ac != nil {
		ac.refs.Store(1)
	}
	return ac
}

func ownContainerBitmap(bc *containerBitmap) *containerBitmap {
	if bc != nil {
		bc.refs.Store(1)
	}
	return bc
}

func TestArrayContainerPool_ReusedSizedContainerLeaksOldContent(t *testing.T) {
	if testRaceEnabled {
		t.Skip("sync.Pool is unreliable under -race")
	}
	ac := getContainerArrayWithLen(4)
	copy(ac.content, []uint16{11, 22, 33, 44})
	ac.release()

	reused := getContainerArrayWithLen(4)
	defer reused.release()
	if reused != ac {
		t.Fatalf("array container pool did not reuse container")
	}
	if !slices.Equal(reused.content, []uint16{0, 0, 0, 0}) {
		t.Fatalf("reused sized array container leaked old content: %v", reused.content)
	}
}

func TestRunContainerPool_ReusedSizedContainerLeaksOldIntervals(t *testing.T) {
	if testRaceEnabled {
		t.Skip("sync.Pool is unreliable under -race")
	}
	rc := newContainerRun()
	rc.iv = slices.Grow(rc.iv, max(4, 4))
	rc.iv = rc.iv[:4]
	rc.iv[0] = interval16{start: 10, length: 1}
	rc.iv[1] = interval16{start: 20, length: 2}
	rc.iv[2] = interval16{start: 30, length: 3}
	rc.iv[3] = interval16{start: 40, length: 4}
	rc.release()

	reused := newContainerRun()
	reused.iv = slices.Grow(reused.iv, max(4, 4))
	reused.iv = reused.iv[:4]
	defer reused.release()
	if reused != rc {
		t.Fatalf("run container pool did not reuse container")
	}
	zero := make([]interval16, 4)
	if !slices.Equal(reused.iv, zero) {
		t.Fatalf("reused sized run container leaked old intervals: %v", reused.iv)
	}
}

func TestRunContainerPool_ReusedLargerLengthPreservesUnreleasedTail(t *testing.T) {
	if testRaceEnabled {
		t.Skip("sync.Pool is unreliable under -race")
	}
	rc := newContainerRun()
	rc.iv = slices.Grow(rc.iv, max(4, 4))
	rc.iv = rc.iv[:4]
	rc.iv[0] = interval16{start: 10, length: 1}
	rc.iv[1] = interval16{start: 20, length: 2}
	rc.iv[2] = interval16{start: 30, length: 3}
	rc.iv[3] = interval16{start: 40, length: 4}
	rc.iv = rc.iv[:2]
	rc.release()

	reused := newContainerRun()
	reused.iv = slices.Grow(reused.iv, max(4, 4))
	reused.iv = reused.iv[:4]
	defer reused.release()
	if reused != rc {
		t.Fatalf("run container pool did not reuse container")
	}
	zero := make([]interval16, 2)
	if !slices.Equal(reused.iv[:2], zero) {
		t.Fatalf("reused larger run container leaked released prefix: %v", reused.iv)
	}
	if !slices.Equal(reused.iv[2:], []interval16{
		{start: 30, length: 3},
		{start: 40, length: 4},
	}) {
		t.Fatalf("reused larger run container unexpectedly normalized unreleased tail: %v", reused.iv)
	}
}

func TestArrayContainerPool_ReusedLargerLengthPreservesUnreleasedTail(t *testing.T) {
	if testRaceEnabled {
		t.Skip("sync.Pool is unreliable under -race")
	}
	ac := getContainerArrayWithLen(4)
	copy(ac.content, []uint16{11, 22, 33, 44})
	ac.content = ac.content[:2]
	ac.release()

	reused := getContainerArrayWithLen(4)
	defer reused.release()
	if reused != ac {
		t.Fatalf("array container pool did not reuse container")
	}
	if !slices.Equal(reused.content[:2], []uint16{0, 0}) {
		t.Fatalf("reused larger array container leaked released prefix: %v", reused.content)
	}
	if !slices.Equal(reused.content[2:], []uint16{33, 44}) {
		t.Fatalf("reused larger array container unexpectedly normalized unreleased tail: %v", reused.content)
	}
}

func TestRunContainerPoolCleanupContract(t *testing.T) {
	t.Run("SizedContainerZeroesReleasedPrefix", func(t *testing.T) {
		backing := [4]interval16{
			{start: 10, length: 1},
			{start: 20, length: 2},
			{start: 30, length: 3},
			{start: 40, length: 4},
		}
		rc := containerRun{iv: backing[:]}

		runContainerPool.Cleanup(&rc)

		zero := [4]interval16{}
		if backing != zero {
			t.Fatalf("run container cleanup leaked released prefix: %v", backing)
		}
		if len(rc.iv) != 0 {
			t.Fatalf("run container cleanup did not reset length: got %d want 0", len(rc.iv))
		}
	})

	t.Run("ShorterLenPreservesUnreleasedTail", func(t *testing.T) {
		backing := [4]interval16{
			{start: 10, length: 1},
			{start: 20, length: 2},
			{start: 30, length: 3},
			{start: 40, length: 4},
		}
		rc := containerRun{iv: backing[:2]}

		runContainerPool.Cleanup(&rc)

		if !slices.Equal(backing[:2], []interval16{{}, {}}) {
			t.Fatalf("run container cleanup leaked released prefix: %v", backing)
		}
		if !slices.Equal(backing[2:], []interval16{
			{start: 30, length: 3},
			{start: 40, length: 4},
		}) {
			t.Fatalf("run container cleanup unexpectedly normalized unreleased tail: %v", backing)
		}
		if len(rc.iv) != 0 {
			t.Fatalf("run container cleanup did not reset length: got %d want 0", len(rc.iv))
		}
	})
}

func TestArrayContainerPoolCleanupContract(t *testing.T) {
	t.Run("SizedContainerZeroesReleasedPrefix", func(t *testing.T) {
		backing := [4]uint16{11, 22, 33, 44}
		ac := containerArray{content: backing[:]}

		containerArrayClassPools[0].Cleanup(&ac)

		if backing != [4]uint16{} {
			t.Fatalf("array container cleanup leaked released prefix: %v", backing)
		}
		if len(ac.content) != 0 {
			t.Fatalf("array container cleanup did not reset length: got %d want 0", len(ac.content))
		}
	})

	t.Run("ShorterLenPreservesUnreleasedTail", func(t *testing.T) {
		backing := [4]uint16{11, 22, 33, 44}
		ac := containerArray{content: backing[:2]}

		containerArrayClassPools[0].Cleanup(&ac)

		if !slices.Equal(backing[:2], []uint16{0, 0}) {
			t.Fatalf("array container cleanup leaked released prefix: %v", backing)
		}
		if !slices.Equal(backing[2:], []uint16{33, 44}) {
			t.Fatalf("array container cleanup unexpectedly normalized unreleased tail: %v", backing)
		}
		if len(ac.content) != 0 {
			t.Fatalf("array container cleanup did not reset length: got %d want 0", len(ac.content))
		}
	})
}

func TestContainerArrayPoolClassifiers(t *testing.T) {
	getCases := []struct {
		size int
		want int
	}{
		{size: -1, want: 0},
		{size: 0, want: 0},
		{size: 1, want: 0},
		{size: 32, want: 0},
		{size: 33, want: 1},
		{size: 64, want: 1},
		{size: 65, want: 2},
		{size: 1024, want: 5},
		{size: 1025, want: 6},
		{size: 4096, want: 7},
		{size: 4097, want: 8},
		{size: maxContainerArrayPoolCapacity, want: len(containerArrayPoolCapacities) - 1},
		{size: maxContainerArrayPoolCapacity + 1, want: -1},
	}
	for _, tc := range getCases {
		if got := containerArrayPoolIndex(tc.size); got != tc.want {
			t.Fatalf("containerArrayPoolIndex(%d) = %d, want %d", tc.size, got, tc.want)
		}
	}

	putCases := []struct {
		capacity int
		want     int
	}{
		{capacity: -1, want: -1},
		{capacity: 0, want: -1},
		{capacity: 31, want: -1},
		{capacity: 32, want: 0},
		{capacity: 33, want: 0},
		{capacity: 63, want: 0},
		{capacity: 64, want: 1},
		{capacity: 65, want: 1},
		{capacity: 1024, want: 5},
		{capacity: 1536, want: 5},
		{capacity: 2048, want: 6},
		{capacity: maxContainerArrayPoolCapacity, want: len(containerArrayPoolCapacities) - 1},
		{capacity: maxContainerArrayPoolCapacity + 1, want: -1},
	}
	for _, tc := range putCases {
		if got := containerArrayPoolPutIndex(tc.capacity); got != tc.want {
			t.Fatalf("containerArrayPoolPutIndex(%d) = %d, want %d", tc.capacity, got, tc.want)
		}
	}
}

func TestContainerOwnershipHelpersRejectNil(t *testing.T) {
	panicCases := []struct {
		name string
		fn   func()
	}{
		{name: "Retain", fn: func() {
			var c container16
			c.retain()
		}},
		{name: "UniquelyOwned", fn: func() {
			var c container16
			c.uniquelyOwned()
		}},
		{name: "Release", fn: func() {
			var c container16
			c.release()
		}},
	}

	for _, tc := range panicCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic")
				}
			}()
			tc.fn()
		})
	}
}

func TestRunContainerToBitmapContainer_CorrectCardinality(t *testing.T) {
	rc := newContainerRunRange(10, 19)
	bc := rc.toBitmapContainer()
	defer bc.release()

	if bc.cardinality != 10 {
		t.Fatalf("unexpected cardinality: got %d want 10", bc.cardinality)
	}

	ac := bc.toArrayContainer()
	defer ac.release()

	want := []uint16{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	if !slices.Equal(ac.content, want) {
		t.Fatalf("unexpected array materialization: got %v want %v", ac.content, want)
	}
}

func TestRunContainerIAndNotArray_NoZeroTail(t *testing.T) {
	rc := newContainerRunRange(10, 19)
	ac := getContainerArrayFromSlice([]uint16{11, 13, 17})
	defer ac.release()

	result := rc.iandNotArray(ac)
	got, ok := result.(*containerArray)
	if !ok {
		t.Fatalf("unexpected result type: %T", result)
	}
	defer got.release()

	want := []uint16{10, 12, 14, 15, 16, 18, 19}
	if !slices.Equal(got.content, want) {
		t.Fatalf("unexpected difference result: got %v want %v", got.content, want)
	}
}

func TestContainerIndexSearchCopyDetachAndRemove(t *testing.T) {
	src := &containerIndex{
		keys: []uint16{1, 3, 7},
		containers: []container16{
			buildContainerArray([]uint16{1, 2}),
			buildContainerRun([]uint16{10, 11, 12}),
			buildContainerBitmap([]uint16{20, 21, 22}),
		},
	}
	defer releaseContainerIndexForTest(src)

	if got := src.binarySearch(0, int64(len(src.keys)), 3); got != 1 {
		t.Fatalf("binarySearch mismatch: got=%d want=1", got)
	}
	if got := src.binarySearch(0, int64(len(src.keys)), 5); got != -3 {
		t.Fatalf("binarySearch insertion mismatch: got=%d want=-3", got)
	}
	if got := src.advanceUntil(7, 0); got != 2 {
		t.Fatalf("advanceUntil mismatch: got=%d want=2", got)
	}
	if got := src.advanceUntil(9, 1); got != len(src.keys) {
		t.Fatalf("advanceUntil past-end mismatch: got=%d want=%d", got, len(src.keys))
	}

	copyDst := new(containerIndex)
	copyDst.copyFrom(src)
	defer releaseContainerIndexForTest(copyDst)
	if !slices.Equal(copyDst.keys, src.keys) {
		t.Fatalf("copyFrom keys mismatch: got=%v want=%v", copyDst.keys, src.keys)
	}
	copyDst.getWritableContainerAtIndex(0).iadd(99)
	if src.getContainerAtIndex(0).contains(99) {
		t.Fatalf("copyFrom result still shares container state with source")
	}

	sharedDst := new(containerIndex)
	sharedDst.copySharedFrom(src)
	defer releaseContainerIndexForTest(sharedDst)
	original := sharedDst.getContainerAtIndex(2)
	writable := sharedDst.getWritableContainerAtIndex(2)
	if writable == original {
		t.Fatalf("copySharedFrom did not detach writable container")
	}
	writable.iadd(42)
	if src.getContainerAtIndex(2).contains(42) {
		t.Fatalf("shared writable container mutation changed source")
	}

	src.removeAtIndex(1)
	if !slices.Equal(src.keys, []uint16{1, 7}) {
		t.Fatalf("removeAtIndex keys mismatch: got=%v want=%v", src.keys, []uint16{1, 7})
	}
}

func TestLargeArraySearchCopyDetachAndRemove(t *testing.T) {
	src := &largeArray{
		keys: []uint32{1, 3, 7},
		containers: []*bitmap32{
			buildBitmap32(1, 2),
			buildBitmap32(10, 11, 12),
			buildBitmap32(20, 21, 22),
		},
	}
	defer releaseLargeArrayForTest(src)

	if got := src.binarySearch(0, int64(len(src.keys)), 3); got != 1 {
		t.Fatalf("binarySearch mismatch: got=%d want=1", got)
	}
	if got := src.binarySearch(0, int64(len(src.keys)), 5); got != -3 {
		t.Fatalf("binarySearch insertion mismatch: got=%d want=-3", got)
	}
	if got := src.advanceUntil(7, 0); got != 2 {
		t.Fatalf("advanceUntil mismatch: got=%d want=2", got)
	}
	if got := src.advanceUntil(9, 1); got != len(src.keys) {
		t.Fatalf("advanceUntil past-end mismatch: got=%d want=%d", got, len(src.keys))
	}

	copyDst := new(largeArray)
	copyDst.copyFrom(src)
	defer releaseLargeArrayForTest(copyDst)
	copyDst.getWritableContainerAtIndex(0).add(99)
	if src.getContainerAtIndex(0).contains(99) {
		t.Fatalf("copyFrom result still shares bitmap32 state with source")
	}

	sharedDst := new(largeArray)
	sharedDst.copySharedFrom(src)
	defer releaseLargeArrayForTest(sharedDst)
	original := sharedDst.getContainerAtIndex(2)
	writable := sharedDst.getWritableContainerAtIndex(2)
	if writable == original {
		t.Fatalf("copySharedFrom did not detach writable bitmap32")
	}
	writable.add(42)
	if src.getContainerAtIndex(2).contains(42) {
		t.Fatalf("shared writable bitmap32 mutation changed source")
	}

	src.removeAtIndex(1)
	if !slices.Equal(src.keys, []uint32{1, 7}) {
		t.Fatalf("removeAtIndex keys mismatch: got=%v want=%v", src.keys, []uint32{1, 7})
	}
}

func TestLowLevelSetKernelsAndSearchHelpers(t *testing.T) {
	set1 := []uint16{1, 3, 5, 7, 9, 11, 1024}
	set2 := []uint16{3, 4, 5, 8, 11, 1024, 2048}

	diffBuf := make([]uint16, len(set1))
	if n := difference(set1, set2, diffBuf); !slices.Equal(diffBuf[:n], []uint16{1, 7, 9}) {
		t.Fatalf("difference mismatch: got=%v want=%v", diffBuf[:n], []uint16{1, 7, 9})
	}

	unionBuf := make([]uint16, len(set1)+len(set2))
	if n := exclusiveUnion2by2(set1, set2, unionBuf); !slices.Equal(unionBuf[:n], []uint16{1, 4, 7, 8, 9, 2048}) {
		t.Fatalf("exclusiveUnion2by2 mismatch: got=%v want=%v", unionBuf[:n], []uint16{1, 4, 7, 8, 9, 2048})
	}

	interBuf := make([]uint16, min(len(set1), len(set2)))
	if n := intersection2by2(set1, set2, interBuf); !slices.Equal(interBuf[:n], []uint16{3, 5, 11, 1024}) {
		t.Fatalf("intersection2by2 mismatch: got=%v want=%v", interBuf[:n], []uint16{3, 5, 11, 1024})
	}
	if got := intersection2by2Cardinality(set1, set2); got != 4 {
		t.Fatalf("intersection2by2Cardinality mismatch: got=%d want=4", got)
	}
	if !intersects2by2(set1, set2) || intersects2by2(set1, []uint16{2, 4, 6}) {
		t.Fatalf("intersects2by2 mismatch")
	}

	skewedSmall := []uint16{10, 200, 400, 800}
	skewedLarge := make([]uint16, 0, 4096)
	for i := uint16(0); i < 4096; i += 2 {
		skewedLarge = append(skewedLarge, i)
	}
	interBuf = make([]uint16, len(skewedSmall))
	if n := intersection2by2(skewedSmall, skewedLarge, interBuf); !slices.Equal(interBuf[:n], []uint16{10, 200, 400, 800}) {
		t.Fatalf("galloping intersection mismatch: got=%v want=%v", interBuf[:n], []uint16{10, 200, 400, 800})
	}
	if got := onesidedgallopingintersect2by2Cardinality(skewedSmall, skewedLarge); got != 4 {
		t.Fatalf("galloping cardinality mismatch: got=%d want=4", got)
	}

	if got := advanceUntil(set2, 0, len(set2), 8); got != 3 {
		t.Fatalf("advanceUntil mismatch: got=%d want=3", got)
	}
	if got := binarySearch(set2, 11); got != 4 {
		t.Fatalf("binarySearch mismatch: got=%d want=4", got)
	}
	if got := binarySearch(set2, 10); got != -5 {
		t.Fatalf("binarySearch insertion mismatch: got=%d want=-5", got)
	}
}

func TestLowLevelHelpersAndOwnershipWrappers(t *testing.T) {
	words := make([]uint64, 4)
	fill(words, 7)
	if !slices.Equal(words, []uint64{7, 7, 7, 7}) {
		t.Fatalf("fill mismatch: got=%v", words)
	}
	fillRange(words, 1, 3, 9)
	if !slices.Equal(words, []uint64{7, 9, 9, 7}) {
		t.Fatalf("fillRange mismatch: got=%v", words)
	}

	rangeContainer := rangeOfOnes(10, 20)
	defer rangeContainer.release()
	assertSameContainerSet(t, rangeContainer, []uint16{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})

	ac := ownContainerArray(&containerArray{})
	if ac.refs.Load() != 1 {
		t.Fatalf("ownContainerArray refs mismatch: got=%d want=1", ac.refs.Load())
	}
	bc := ownContainerBitmap(&containerBitmap{bitmap: make([]uint64, bitmapContainerWords)})
	if bc.refs.Load() != 1 {
		t.Fatalf("ownContainerBitmap refs mismatch: got=%d want=1", bc.refs.Load())
	}
	ac.release()
	bc.release()
}

func TestLowLevelSearchHelperEdgeCases(t *testing.T) {
	ci := &containerIndex{
		keys: []uint16{1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 69, 73, 77},
		containers: []container16{
			buildContainerArray([]uint16{1}),
			buildContainerArray([]uint16{5}),
			buildContainerArray([]uint16{9}),
			buildContainerArray([]uint16{13}),
			buildContainerArray([]uint16{17}),
			buildContainerArray([]uint16{21}),
			buildContainerArray([]uint16{25}),
			buildContainerArray([]uint16{29}),
			buildContainerArray([]uint16{33}),
			buildContainerArray([]uint16{37}),
			buildContainerArray([]uint16{41}),
			buildContainerArray([]uint16{45}),
			buildContainerArray([]uint16{49}),
			buildContainerArray([]uint16{53}),
			buildContainerArray([]uint16{57}),
			buildContainerArray([]uint16{61}),
			buildContainerArray([]uint16{65}),
			buildContainerArray([]uint16{69}),
			buildContainerArray([]uint16{73}),
			buildContainerArray([]uint16{77}),
		},
	}
	defer releaseContainerIndexForTest(ci)

	if got := ci.binarySearch(0, int64(len(ci.keys)), 49); got != 12 {
		t.Fatalf("containerIndex binarySearch exact mismatch: got=%d want=12", got)
	}
	if got := ci.binarySearch(0, int64(len(ci.keys)), 50); got != -14 {
		t.Fatalf("containerIndex binarySearch insertion mismatch: got=%d want=-14", got)
	}
	if got := ci.getIndex(100); got != -21 {
		t.Fatalf("containerIndex getIndex tail insertion mismatch: got=%d want=-21", got)
	}
	if got := ci.advanceUntil(5, 0); got != 1 {
		t.Fatalf("containerIndex advanceUntil immediate mismatch: got=%d want=1", got)
	}
	if got := ci.advanceUntil(53, 0); got != 13 {
		t.Fatalf("containerIndex advanceUntil galloping mismatch: got=%d want=13", got)
	}
	if got := ci.advanceUntil(54, 0); got != 14 {
		t.Fatalf("containerIndex advanceUntil upper-bound mismatch: got=%d want=14", got)
	}
	if got := ci.advanceUntil(100, 0); got != len(ci.keys) {
		t.Fatalf("containerIndex advanceUntil end mismatch: got=%d want=%d", got, len(ci.keys))
	}

	la := &largeArray{
		keys: []uint32{1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 69, 73, 77},
		containers: []*bitmap32{
			buildBitmap32(1), buildBitmap32(5), buildBitmap32(9), buildBitmap32(13), buildBitmap32(17),
			buildBitmap32(21), buildBitmap32(25), buildBitmap32(29), buildBitmap32(33), buildBitmap32(37),
			buildBitmap32(41), buildBitmap32(45), buildBitmap32(49), buildBitmap32(53), buildBitmap32(57),
			buildBitmap32(61), buildBitmap32(65), buildBitmap32(69), buildBitmap32(73), buildBitmap32(77),
		},
	}
	defer releaseLargeArrayForTest(la)

	if got := la.binarySearch(0, int64(len(la.keys)), 49); got != 12 {
		t.Fatalf("largeArray binarySearch exact mismatch: got=%d want=12", got)
	}
	if got := la.binarySearch(0, int64(len(la.keys)), 50); got != -14 {
		t.Fatalf("largeArray binarySearch insertion mismatch: got=%d want=-14", got)
	}
	if got := la.getIndex(100); got != -21 {
		t.Fatalf("largeArray getIndex tail insertion mismatch: got=%d want=-21", got)
	}
	if got := la.advanceUntil(5, 0); got != 1 {
		t.Fatalf("largeArray advanceUntil immediate mismatch: got=%d want=1", got)
	}
	if got := la.advanceUntil(53, 0); got != 13 {
		t.Fatalf("largeArray advanceUntil galloping mismatch: got=%d want=13", got)
	}
	if got := la.advanceUntil(54, 0); got != 14 {
		t.Fatalf("largeArray advanceUntil upper-bound mismatch: got=%d want=14", got)
	}
	if got := la.advanceUntil(100, 0); got != len(la.keys) {
		t.Fatalf("largeArray advanceUntil end mismatch: got=%d want=%d", got, len(la.keys))
	}
}

func TestIndexAppendInsertionKeepsOrdering(t *testing.T) {
	ci := &containerIndex{
		keys: []uint16{1, 3},
		containers: []container16{
			buildContainerArray([]uint16{1}),
			buildContainerArray([]uint16{3}),
		},
	}
	defer releaseContainerIndexForTest(ci)

	ciTail := buildContainerArray([]uint16{5})
	ci.insertNewKeyValueAt(2, 5, ciTail)
	if !slices.Equal(ci.keys, []uint16{1, 3, 5}) {
		t.Fatalf("containerIndex append insertion keys mismatch: got=%v want=%v", ci.keys, []uint16{1, 3, 5})
	}
	if !ci.getContainerAtIndex(2).contains(5) {
		t.Fatalf("containerIndex append insertion lost tail container value")
	}

	la := &largeArray{
		keys: []uint32{1, 3},
		containers: []*bitmap32{
			buildBitmap32(1),
			buildBitmap32(3),
		},
	}
	defer releaseLargeArrayForTest(la)

	laTail := buildBitmap32(5)
	la.insertNewKeyValueAt(2, 5, laTail)
	if !slices.Equal(la.keys, []uint32{1, 3, 5}) {
		t.Fatalf("largeArray append insertion keys mismatch: got=%v want=%v", la.keys, []uint32{1, 3, 5})
	}
	if !la.getContainerAtIndex(2).contains(5) {
		t.Fatalf("largeArray append insertion lost tail bitmap32 value")
	}
}

func TestLowLevelAppendCopyAliasesAndPopcountHelpers(t *testing.T) {
	src := containerIndex{
		keys: []uint16{1, 3, 7},
		containers: []container16{
			buildContainerArray([]uint16{1, 2}),
			buildContainerRun([]uint16{10, 11, 12}),
			buildContainerBitmap([]uint16{20, 21, 22}),
		},
	}
	defer releaseContainerIndexForTest(&src)

	alias := &containerIndex{
		keys:       src.keys[:2],
		containers: src.containers[:2],
	}
	if !src.aliases(alias) {
		t.Fatalf("containerIndex aliases must detect shared backing")
	}

	independent := &containerIndex{
		keys:       []uint16{1, 3},
		containers: []container16{buildContainerArray([]uint16{1, 2})},
	}
	defer releaseContainerIndexForTest(independent)
	if src.aliases(independent) {
		t.Fatalf("containerIndex aliases must reject distinct backing")
	}

	var dst containerIndex
	dst.appendCopy(src, 0)
	dst.appendCopyMany(src, 1, 3)
	defer releaseContainerIndexForTest(&dst)

	if !slices.Equal(dst.keys, src.keys) {
		t.Fatalf("appendCopy/appendCopyMany keys mismatch: got=%v want=%v", dst.keys, src.keys)
	}
	for i := range src.keys {
		assertSameContainerSet(t, dst.getContainerAtIndex(i), containerToSlice(src.getContainerAtIndex(i)))
	}
	dst.getWritableContainerAtIndex(0).iadd(99)
	if src.getContainerAtIndex(0).contains(99) {
		t.Fatalf("appendCopy result still shares container state with source")
	}

	maskA := []uint64{0b11110000, 0b10101010}
	maskB := []uint64{0b00111100, 0b11110000}
	if got := popcntMaskSlice(maskA, maskB); got != 4 {
		t.Fatalf("popcntMaskSlice mismatch: got=%d want=4", got)
	}
	if got := popcntXorSlice(maskA, maskB); got != 8 {
		t.Fatalf("popcntXorSlice mismatch: got=%d want=8", got)
	}
}

func TestRangeOfOnesContracts(t *testing.T) {
	t.Run("EfficientRepresentations", func(t *testing.T) {
		single := rangeOfOnes(10, 10)
		defer single.release()
		assertSameContainerSet(t, single, []uint16{10})

		full := rangeOfOnes(0, MaxUint16)
		defer full.release()
		if _, ok := full.(*containerRun); !ok {
			t.Fatalf("full rangeOfOnes must stay run, got %T", full)
		}
		if !full.isFull() {
			t.Fatalf("full rangeOfOnes must report full")
		}
	})

	panicCases := []struct {
		name string
		fn   func()
	}{
		{name: "StartTooLarge", fn: func() { rangeOfOnes(MaxUint16+1, MaxUint16+1) }},
		{name: "LastTooLarge", fn: func() { rangeOfOnes(0, MaxUint16+1) }},
		{name: "StartNegative", fn: func() { rangeOfOnes(-1, 0) }},
		{name: "LastNegative", fn: func() { rangeOfOnes(0, -1) }},
	}

	for _, tc := range panicCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic")
				}
			}()
			tc.fn()
		})
	}
}

/**/

var containerEqualsSink bool

func oldArrayEquals(ac *containerArray, o container16) bool {
	if other, ok := o.(*containerArray); ok {
		if ac == other {
			return true
		}
		if len(other.content) != len(ac.content) {
			return false
		}
		for i, v := range ac.content {
			if v != other.content[i] {
				return false
			}
		}
		return true
	}

	if o.getCardinality() != ac.getCardinality() {
		return false
	}
	ait := ac.getShortIterator()
	var bit shortPeekable
	switch other := o.(type) {
	case *containerBitmap:
		bit = other.getShortIterator()
	case *containerRun:
		bit = other.getShortIterator()
	default:
		panic("unsupported container16 type")
	}
	for ait.hasNext() {
		if bit.next() != ait.next() {
			return false
		}
	}
	return true
}

func oldBitmapEquals(bc *containerBitmap, o container16) bool {
	if other, ok := o.(*containerBitmap); ok {
		if other.cardinality != bc.cardinality {
			return false
		}
		return bitmapEquals(bc.bitmap, other.bitmap)
	}

	if bc.getCardinality() != o.getCardinality() {
		return false
	}
	var ait shortPeekable
	switch other := o.(type) {
	case *containerArray:
		ait = other.getShortIterator()
	case *containerRun:
		ait = other.getShortIterator()
	default:
		panic("unsupported container16 type")
	}
	bit := bc.getShortIterator()
	for ait.hasNext() {
		if bit.next() != ait.next() {
			return false
		}
	}
	return true
}

func oldRunEquals(rc *containerRun, o container16) bool {
	if other, ok := o.(*containerRun); ok {
		if rc == other {
			return true
		}
		if len(other.iv) != len(rc.iv) {
			return false
		}
		for i, v := range rc.iv {
			if v != other.iv[i] {
				return false
			}
		}
		return true
	}

	if o.getCardinality() != rc.getCardinality() {
		return false
	}
	var bit shortPeekable
	switch other := o.(type) {
	case *containerArray:
		bit = other.getShortIterator()
	case *containerBitmap:
		bit = other.getShortIterator()
	default:
		panic("unsupported container16 type")
	}
	rit := rc.getShortIterator()
	for rit.hasNext() {
		if bit.next() != rit.next() {
			return false
		}
	}
	return true
}

func benchmarkContainerEquals(b *testing.B, fn func() bool) {
	b.Helper()
	b.ReportAllocs()
	var result bool
	for i := 0; i < b.N; i++ {
		result = fn()
	}
	containerEqualsSink = result
}

func BenchmarkContainerCrossTypeEquals(b *testing.B) {
	arrayValues := make([]uint16, arrayDefaultMaxSize)
	for i := range arrayValues {
		arrayValues[i] = uint16(i * 2)
	}

	arrayEqual := buildContainerArray(arrayValues)
	defer arrayEqual.release()
	arrayShifted := buildContainerArray(append([]uint16(nil), arrayValues...))
	arrayShifted.(*containerArray).content[0]++
	defer arrayShifted.release()

	bitmapEqual := buildContainerBitmap(arrayValues)
	defer bitmapEqual.release()
	bitmapShifted := buildContainerBitmap(containerToSlice(arrayShifted))
	defer bitmapShifted.release()

	runValues := make([]uint16, 0, 2048)
	for start := 0; start < 8192; start += 8 {
		for value := start; value < start+3; value++ {
			runValues = append(runValues, uint16(value))
		}
	}

	runEqual := buildContainerRun(runValues).(*containerRun)
	defer runEqual.release()
	runShiftedValues := append([]uint16(nil), runValues...)
	runShiftedValues[0]++
	runShifted := buildContainerRun(runShiftedValues).(*containerRun)
	defer runShifted.release()

	bitmapRunEqual := buildContainerBitmap(runValues)
	defer bitmapRunEqual.release()
	bitmapRunShifted := buildContainerBitmap(runShiftedValues)
	defer bitmapRunShifted.release()

	b.Run("ArrayBitmap/Equal/New", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return arrayEqual.equals(bitmapEqual)
		})
	})
	b.Run("ArrayBitmap/Equal/Old", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return oldArrayEquals(arrayEqual.(*containerArray), bitmapEqual)
		})
	})
	b.Run("ArrayBitmap/Mismatch/New", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return arrayEqual.equals(bitmapShifted)
		})
	})
	b.Run("ArrayBitmap/Mismatch/Old", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return oldArrayEquals(arrayEqual.(*containerArray), bitmapShifted)
		})
	})

	b.Run("ArrayRun/Equal/New", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return arrayEqual.equals(runEqual)
		})
	})
	b.Run("ArrayRun/Equal/Old", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return oldArrayEquals(arrayEqual.(*containerArray), runEqual)
		})
	})
	b.Run("ArrayRun/Mismatch/New", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return arrayEqual.equals(runShifted)
		})
	})
	b.Run("ArrayRun/Mismatch/Old", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return oldArrayEquals(arrayEqual.(*containerArray), runShifted)
		})
	})

	b.Run("BitmapRun/Equal/New", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return bitmapRunEqual.equals(runEqual)
		})
	})
	b.Run("BitmapRun/Equal/Old", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return oldBitmapEquals(bitmapRunEqual.(*containerBitmap), runEqual)
		})
	})
	b.Run("BitmapRun/Mismatch/New", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return bitmapRunEqual.equals(runShifted)
		})
	})
	b.Run("BitmapRun/Mismatch/Old", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return oldBitmapEquals(bitmapRunEqual.(*containerBitmap), runShifted)
		})
	})

	b.Run("RunBitmap/Equal/New", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return runEqual.equals(bitmapRunEqual)
		})
	})
	b.Run("RunBitmap/Equal/Old", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return oldRunEquals(runEqual, bitmapRunEqual)
		})
	})
	b.Run("RunBitmap/Mismatch/New", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return runEqual.equals(bitmapRunShifted)
		})
	})
	b.Run("RunBitmap/Mismatch/Old", func(b *testing.B) {
		benchmarkContainerEquals(b, func() bool {
			return oldRunEquals(runEqual, bitmapRunShifted)
		})
	})
}
