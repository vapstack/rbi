package posting

import (
	"bytes"
	"encoding/binary"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

func collectIntIterator32(it *intIterator) []uint32 {
	out := make([]uint32, 0, 32)
	for it.hasNext() {
		out = append(out, it.next())
	}
	return out
}

func TestBitmapCloneSharedIntoPreservesSourceOnMutation(t *testing.T) {
	src := newBitmap32()
	src.addRange(0, 512)
	src.addRange(1<<16, (1<<16)+512)
	src.addRange(3<<16, (3<<16)+32)
	src.runOptimize()

	want := src.clone()

	dst := newBitmap32()
	src.cloneSharedInto(dst)

	dst.add(777)
	dst.remove(10)
	dst.remove((1 << 16) + 3)
	dst.or(newBitmap32())
	dst.runOptimize()

	if !src.equals(want) {
		t.Fatalf("shared clone mutation changed source bitmap")
	}
	if dst.equals(src) {
		t.Fatalf("shared clone mutation did not change destination bitmap")
	}
	if src.contains(777) {
		t.Fatalf("source unexpectedly contains added value")
	}
	if !src.contains(10) || !src.contains((1<<16)+3) {
		t.Fatalf("source lost existing values after destination mutation")
	}
}

func TestBitmapPool_ReusedBitmapStartsEmpty(t *testing.T) {
	rb := acquireBitmap()
	rb.add(1)
	rb.add(2)
	for i := uint32(0); i < 5000; i++ {
		rb.add((1 << 16) | (i << 1))
	}
	rb.addRange(uint64(2<<16), uint64((2<<16)+1024))
	releaseBitmap(rb)

	reused := acquireBitmap()
	defer releaseBitmap(reused)
	if !testRaceEnabled && reused != rb {
		t.Fatalf("bitmap pool did not reuse bitmap")
	}

	reused.add(7)
	reused.add((3 << 16) | 9)
	if reused.cardinality() != 2 {
		t.Fatalf("reused bitmap has stale cardinality: %d", reused.cardinality())
	}
	if !reused.contains(7) || !reused.contains((3<<16)|9) {
		t.Fatalf("reused bitmap lost new values")
	}
	if reused.contains(1) || reused.contains(2) || reused.contains(1<<16) || reused.contains(2<<16) {
		t.Fatalf("reused bitmap leaked old contents")
	}
}

func TestIntIteratorPool_ReusedIteratorResetsEmbeddedState(t *testing.T) {
	arrayBitmap := newBitmap32()
	arrayBitmap.add(1)
	arrayBitmap.add(2)
	arrayBitmap.add(3)

	it := acquireIntIterator(arrayBitmap)
	if got := it.next(); got != 1 {
		t.Fatalf("array iterator first value: got=%d", got)
	}
	releaseIntIterator(it)

	runBitmap := newBitmap32()
	runBitmap.addRange(uint64(1<<16), uint64((1<<16)+4))

	reused := acquireIntIterator(runBitmap)
	if !testRaceEnabled && reused != it {
		t.Fatalf("intIterator pool did not reuse iterator")
	}
	if got := reused.peekNext(); got != 1<<16 {
		t.Fatalf("reused iterator leaked stale position: got=%d", got)
	}
	gotRun := collectIntIterator32(reused)
	wantRun := []uint32{1 << 16, (1 << 16) + 1, (1 << 16) + 2, (1 << 16) + 3}
	if !slices.Equal(gotRun, wantRun) {
		t.Fatalf("run iteration mismatch after reuse: got=%v want=%v", gotRun, wantRun)
	}
	releaseIntIterator(reused)

	bitmapBitmap := newBitmap32()
	for i := uint32(0); i < 5000; i++ {
		bitmapBitmap.add((2 << 16) | (i << 1))
	}

	reused2 := acquireIntIterator(bitmapBitmap)
	defer releaseIntIterator(reused2)
	if !testRaceEnabled && reused2 != it {
		t.Fatalf("intIterator pool did not reuse iterator on second pass")
	}
	if got := reused2.peekNext(); got != 2<<16 {
		t.Fatalf("bitmap iterator head mismatch after reuse: got=%d", got)
	}
	gotBitmap := collectIntIterator32(reused2)
	if len(gotBitmap) != 5000 || gotBitmap[0] != 2<<16 || gotBitmap[len(gotBitmap)-1] != (2<<16)|((5000-1)<<1) {
		t.Fatalf("bitmap iteration mismatch after reuse: first=%d last=%d len=%d", gotBitmap[0], gotBitmap[len(gotBitmap)-1], len(gotBitmap))
	}
}

func TestReleaseBitmap_SharedCloneSurvivesPoolReuse(t *testing.T) {
	src := newBitmap32()
	src.add(7)
	src.add(9)
	src.addRange(uint64(1<<16), uint64((1<<16)+128))

	clone := src.cloneSharedInto(newBitmap32())
	releaseBitmap(src)

	reused := acquireBitmap()
	reused.add(12345)
	releaseBitmap(reused)

	if !clone.contains(7) || !clone.contains(9) || !clone.contains(1<<16) || !clone.contains((1<<16)+127) {
		t.Fatalf("shared clone lost data after source release and pool reuse")
	}
	if clone.contains(12345) {
		t.Fatalf("pool reuse corrupted shared clone")
	}
	releaseBitmap(clone)
}

func TestBitmap32BasicsAndBoundaries(t *testing.T) {
	rb := newBitmap32()
	defer releaseBitmap32(rb)

	if !rb.isEmpty() {
		t.Fatalf("new bitmap32 must be empty")
	}

	if !rb.checkedAdd(1) || rb.checkedAdd(1) {
		t.Fatalf("checkedAdd duplicate detection mismatch")
	}

	rb.addMany([]uint32{3, 5, 7, 65535, 65536, 65538, MaxUint32})
	rb.addRange(10, 15)
	rb.addRange(65534, 65537)
	rb.addRange(uint64(MaxUint32), MaxRange)
	rb.addRange(20, 20)

	want := canonicalUint32s([]uint32{
		1, 3, 5, 7,
		10, 11, 12, 13, 14,
		65534, 65535, 65536, 65538,
		MaxUint32,
	})
	assertSameBitmap32Set(t, rb, want)

	if got := rb.minimum(); got != want[0] {
		t.Fatalf("minimum mismatch: got=%d want=%d", got, want[0])
	}
	if got := rb.maximum(); got != want[len(want)-1] {
		t.Fatalf("maximum mismatch: got=%d want=%d", got, want[len(want)-1])
	}
	if rb.sizeInBytes() == 0 {
		t.Fatalf("sizeInBytes must be positive for non-empty bitmap")
	}

	rb.remove(65535)
	rb.remove(3)
	rb.remove(MaxUint32)
	assertSameBitmap32Set(t, rb, []uint32{
		1, 5, 7,
		10, 11, 12, 13, 14,
		65534, 65536, 65538,
	})
}

func TestBitmap32AddManyHandlesUnsortedAndDuplicates(t *testing.T) {
	rb := newBitmap32()
	defer releaseBitmap32(rb)

	input := []uint32{
		1 << 16, 7, 5, 1<<16 | 9, 7,
		2, 1<<16 | 1, 5, 1 << 16, 3,
	}
	rb.addMany(input)

	assertSameBitmap32Set(t, rb, []uint32{
		2, 3, 5, 7,
		1 << 16, 1<<16 | 1, 1<<16 | 9,
	})
}

func TestBitmap32AddManySortedFullContainerMinimizesToRun(t *testing.T) {
	makeBatch := func(start, end int) []uint32 {
		out := make([]uint32, end-start)
		for i := range out {
			out[i] = uint32(start + i)
		}
		return out
	}

	t.Run("Fresh", func(t *testing.T) {
		rb := newBitmap32()
		defer releaseBitmap32(rb)

		want := makeBatch(0, maxCapacity)
		rb.addMany(want)
		assertSameBitmap32Set(t, rb, want)

		if got := rb.highlowcontainer.size(); got != 1 {
			t.Fatalf("unexpected container count: got=%d want=1", got)
		}
		if _, ok := rb.highlowcontainer.getContainerAtIndex(0).(*containerRun); !ok {
			t.Fatalf("fresh sorted full container must minimize to run, got %T", rb.highlowcontainer.getContainerAtIndex(0))
		}
	})

	t.Run("ExistingArrayAndBitmap", func(t *testing.T) {
		rb := newBitmap32()
		defer releaseBitmap32(rb)

		want := makeBatch(0, maxCapacity)
		rb.addMany(want[:8])
		rb.addMany(want[8:])
		assertSameBitmap32Set(t, rb, want)

		if _, ok := rb.highlowcontainer.getContainerAtIndex(0).(*containerRun); !ok {
			t.Fatalf("sorted bulk fill through existing array must minimize to run, got %T", rb.highlowcontainer.getContainerAtIndex(0))
		}

		rb2 := newBitmap32()
		defer releaseBitmap32(rb2)
		rb2.addMany(want[:arrayDefaultMaxSize+32])
		rb2.addMany(want[arrayDefaultMaxSize+32:])
		assertSameBitmap32Set(t, rb2, want)

		if _, ok := rb2.highlowcontainer.getContainerAtIndex(0).(*containerRun); !ok {
			t.Fatalf("sorted bulk fill through existing bitmap must minimize to run, got %T", rb2.highlowcontainer.getContainerAtIndex(0))
		}
	})
}

func TestMergeArrayWithSorted32ReusesCapacity(t *testing.T) {
	cases := []struct {
		name  string
		left  []uint16
		batch []uint32
		want  []uint16
	}{
		{
			name:  "InterleavedOverlap",
			left:  []uint16{1, 3, 5},
			batch: []uint32{3, 4, 7, 8, 8},
			want:  []uint16{1, 3, 4, 5, 7, 8},
		},
		{
			name:  "DuplicateBatchNoGrowth",
			left:  []uint16{1, 3, 5},
			batch: []uint32{1, 1, 3, 5, 5},
			want:  []uint16{1, 3, 5},
		},
		{
			name:  "PrefixInsertAndOverlap",
			left:  []uint16{10, 20, 30},
			batch: []uint32{1, 1, 5, 20},
			want:  []uint16{1, 5, 10, 20, 30},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ac := newContainerArrayCapacity(len(tc.want) + 4)
			defer releaseContainerArray(ac)
			ac.content = append(ac.content[:0], tc.left...)

			base := &ac.content[0]
			got := mergeArrayWithSorted32(ac, tc.batch)
			arr, ok := got.(*containerArray)
			if !ok {
				t.Fatalf("unexpected result type: %T", got)
			}
			if arr != ac {
				t.Fatalf("merge must reuse array container instance")
			}
			if &arr.content[0] != base {
				t.Fatalf("merge must reuse existing array storage when capacity is sufficient")
			}
			if !slices.Equal(arr.content, tc.want) {
				t.Fatalf("unexpected merged content: got=%v want=%v", arr.content, tc.want)
			}
		})
	}
}

func TestBitmap32SortedHelperPaths(t *testing.T) {
	t.Run("LoadManySorted64", func(t *testing.T) {
		rb := newBitmap32()
		defer releaseBitmap32(rb)

		rb.loadManySorted64([]uint64{
			3, 5,
			1<<16 | 7, 1<<16 | 9,
			2<<16 | 11,
		})
		assertSameBitmap32Set(t, rb, []uint32{
			3, 5,
			1<<16 | 7, 1<<16 | 9,
			2<<16 | 11,
		})
	})

	t.Run("AddSortedSameHighbitsArrayBitmapAndRun", func(t *testing.T) {
		arrayRB := newBitmap32()
		defer releaseBitmap32(arrayRB)
		arrayRB.addMany([]uint32{1, 3, 5})
		arrayRB.addSortedSameHighbits(0, []uint32{3, 4, 7, 8, 8})
		assertSameBitmap32Set(t, arrayRB, []uint32{1, 3, 4, 5, 7, 8})
		if _, ok := arrayRB.highlowcontainer.getContainerAtIndex(0).(*containerArray); !ok {
			t.Fatalf("array merge path must keep array for sparse result, got %T", arrayRB.highlowcontainer.getContainerAtIndex(0))
		}

		bitmapRB := newBitmap32()
		defer releaseBitmap32(bitmapRB)
		dense := make([]uint32, 0, arrayDefaultMaxSize+64)
		for i := 0; i < arrayDefaultMaxSize+64; i++ {
			dense = append(dense, 2<<16|uint32(i*2))
		}
		bitmapRB.addMany(dense)
		bitmapRB.addSortedSameHighbits(2, []uint32{2<<16 | 1, 2<<16 | 3, 2<<16 | 5})
		if _, ok := bitmapRB.highlowcontainer.getContainerAtIndex(0).(*containerBitmap); !ok {
			t.Fatalf("bitmap merge path must keep bitmap for dense result, got %T", bitmapRB.highlowcontainer.getContainerAtIndex(0))
		}
		if !bitmapRB.contains(2<<16|1) || !bitmapRB.contains(2<<16|3) || !bitmapRB.contains(2<<16|5) {
			t.Fatalf("bitmap merge path lost sorted additions")
		}

		runRB := newBitmap32()
		defer releaseBitmap32(runRB)
		runRB.addRange(1<<16, (1<<16)+8)
		runRB.runOptimize()
		if _, ok := runRB.highlowcontainer.getContainerAtIndex(0).(*containerRun); !ok {
			t.Fatalf("expected run container before addSortedSameHighbits, got %T", runRB.highlowcontainer.getContainerAtIndex(0))
		}
		runRB.addSortedSameHighbits(1, []uint32{1<<16 | 3, 1<<16 | 4, 1<<16 | 4, 1<<16 | 20, 1<<16 | 21})
		assertSameBitmap32Set(t, runRB, []uint32{
			1 << 16, 1<<16 | 1, 1<<16 | 2, 1<<16 | 3,
			1<<16 | 4, 1<<16 | 5, 1<<16 | 6, 1<<16 | 7,
			1<<16 | 20, 1<<16 | 21,
		})
		if _, ok := runRB.highlowcontainer.getContainerAtIndex(0).(*containerRun); !ok {
			t.Fatalf("run merge path must stay run for range-friendly result, got %T", runRB.highlowcontainer.getContainerAtIndex(0))
		}
	})
}

func TestBitmap32SetOpsAndRunOptimize(t *testing.T) {
	left := buildBitmap32(1, 3, 5, 7, 1<<16|9, 2<<16|11)
	right := buildBitmap32(3, 4, 5, 8, 1<<16|9, 3<<16|1)
	defer releaseBitmap32(left)
	defer releaseBitmap32(right)

	wantAnd := intersectUint32(bitmap32ToSlice(left), bitmap32ToSlice(right))
	wantOr := unionUint32(bitmap32ToSlice(left), bitmap32ToSlice(right))
	wantXor := xorUint32(bitmap32ToSlice(left), bitmap32ToSlice(right))
	wantAndNot := differenceUint32(bitmap32ToSlice(left), bitmap32ToSlice(right))

	andBitmap := left.clone()
	defer releaseBitmap32(andBitmap)
	andBitmap.and(right)
	assertSameBitmap32Set(t, andBitmap, wantAnd)

	orBitmap := left.clone()
	defer releaseBitmap32(orBitmap)
	orBitmap.or(right)
	assertSameBitmap32Set(t, orBitmap, wantOr)

	xorBitmap := left.clone()
	defer releaseBitmap32(xorBitmap)
	xorBitmap.xor(right)
	assertSameBitmap32Set(t, xorBitmap, wantXor)

	andNotBitmap := left.clone()
	defer releaseBitmap32(andNotBitmap)
	andNotBitmap.andNot(right)
	assertSameBitmap32Set(t, andNotBitmap, wantAndNot)

	xorHelper := xorBitmap32(left, right)
	defer releaseBitmap32(xorHelper)
	assertSameBitmap32Set(t, xorHelper, wantXor)

	if got := left.intersects(right); got != (len(wantAnd) > 0) {
		t.Fatalf("intersects mismatch: got=%v want=%v", got, len(wantAnd) > 0)
	}
	if got := left.andCardinality(right); got != uint64(len(wantAnd)) {
		t.Fatalf("andCardinality mismatch: got=%d want=%d", got, len(wantAnd))
	}

	sparse := newBitmap32()
	defer releaseBitmap32(sparse)
	sparse.addMany([]uint32{1, 3, 7, 11})
	sparse.runOptimize()
	if _, ok := sparse.highlowcontainer.getContainerAtIndex(0).(*containerArray); !ok {
		t.Fatalf("sparse container should stay array after runOptimize")
	}

	runFriendly := newBitmap32()
	defer releaseBitmap32(runFriendly)
	runFriendly.addRange(1<<16, (1<<16)+1024)
	runFriendly.runOptimize()
	if _, ok := runFriendly.highlowcontainer.getContainerAtIndex(0).(*containerRun); !ok {
		t.Fatalf("run-friendly container should become run after runOptimize")
	}

	dense := newBitmap32()
	defer releaseBitmap32(dense)
	for i := uint32(0); i < 5000; i++ {
		dense.add((2 << 16) | (i << 1))
	}
	dense.runOptimize()
	if _, ok := dense.highlowcontainer.getContainerAtIndex(0).(*containerBitmap); !ok {
		t.Fatalf("dense non-run-friendly container should stay bitmap after runOptimize")
	}
}

func TestBitmap32IteratorsAcrossMixedContainers(t *testing.T) {
	rb := newBitmap32()
	defer releaseBitmap32(rb)

	rb.addMany([]uint32{1, 3, 7})
	rb.addRange(1<<16, (1<<16)+5)
	for i := uint32(0); i < 5000; i++ {
		rb.add((2 << 16) | (i << 1))
	}
	rb.runOptimize()

	want := bitmap32ToSlice(rb)

	it := rb.iterator()
	defer releaseIterator(it)
	if got := it.peekNext(); got != want[0] {
		t.Fatalf("peekNext mismatch: got=%d want=%d", got, want[0])
	}
	it.advanceIfNeeded(1 << 16)
	if got := it.peekNext(); got != 1<<16 {
		t.Fatalf("advanceIfNeeded mismatch: got=%d want=%d", got, 1<<16)
	}

	var gotIter []uint32
	for it.hasNext() {
		gotIter = append(gotIter, it.next())
	}
	if !slices.Equal(gotIter, want[3:]) {
		t.Fatalf("iterator mismatch after advance: got=%v want=%v", gotIter, want[3:])
	}

	many := rb.manyIterator()
	defer releaseManyIterator(many)
	buf := make([]uint32, 257)
	var gotMany []uint32
	for {
		n := many.nextMany(buf)
		if n == 0 {
			break
		}
		gotMany = append(gotMany, buf[:n]...)
	}
	if !slices.Equal(gotMany, want) {
		t.Fatalf("nextMany mismatch: got=%v want=%v", gotMany, want)
	}

	many64 := rb.manyIterator()
	defer releaseManyIterator(many64)
	buf64 := make([]uint64, 257)
	var gotMany64 []uint64
	for {
		n := many64.nextMany64(0, buf64)
		if n == 0 {
			break
		}
		gotMany64 = append(gotMany64, buf64[:n]...)
	}
	want64 := make([]uint64, len(want))
	for i, v := range want {
		want64[i] = uint64(v)
	}
	if !slices.Equal(gotMany64, want64) {
		t.Fatalf("nextMany64 mismatch: got=%v want=%v", gotMany64, want64)
	}
}

func TestBitmap32IntersectsAndIteratorAdvanceEdgeCases(t *testing.T) {
	t.Run("IntersectsAdvancePaths", func(t *testing.T) {
		tests := []struct {
			name  string
			left  []uint32
			right []uint32
			want  bool
		}{
			{
				name:  "AdvanceLeftToMatch",
				left:  []uint32{1, 1<<16 | 1, 4<<16 | 7},
				right: []uint32{4<<16 | 7},
				want:  true,
			},
			{
				name:  "AdvanceRightToMatch",
				left:  []uint32{5<<16 | 9},
				right: []uint32{1, 2<<16 | 3, 5<<16 | 9},
				want:  true,
			},
			{
				name:  "SameKeyNoIntersection",
				left:  []uint32{7<<16 | 1},
				right: []uint32{7<<16 | 2},
				want:  false,
			},
			{
				name:  "AdvanceToEndWithoutMatch",
				left:  []uint32{1, 3, 5},
				right: []uint32{8<<16 | 1},
				want:  false,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				left := buildBitmap32(tc.left...)
				right := buildBitmap32(tc.right...)
				defer releaseBitmap32(left)
				defer releaseBitmap32(right)
				if got := left.intersects(right); got != tc.want {
					t.Fatalf("intersects mismatch: got=%v want=%v", got, tc.want)
				}
			})
		}
	})

	t.Run("IteratorAdvanceNoopAndExhaustion", func(t *testing.T) {
		rb := buildBitmap32(1, 4, 7, 1<<16|1)
		defer releaseBitmap32(rb)

		it := rb.iterator()
		defer releaseIterator(it)

		it.advanceIfNeeded(0)
		if got := it.peekNext(); got != 1 {
			t.Fatalf("advanceIfNeeded no-op mismatch: got=%d want=1", got)
		}

		it.advanceIfNeeded(8)
		if got := it.peekNext(); got != 1<<16|1 {
			t.Fatalf("advanceIfNeeded exhaustion-of-container mismatch: got=%d want=%d", got, 1<<16|1)
		}

		it.advanceIfNeeded(2 << 16)
		if it.hasNext() {
			t.Fatalf("advanceIfNeeded past end must exhaust iterator")
		}
	})
}

func TestBitmap32WireEncodingAndReadRejectInvalidPayloads(t *testing.T) {
	arrayContainer := buildContainerArray([]uint16{1, 3, 7})
	defer releaseContainer(arrayContainer)
	sparseBitmapContainer := buildContainerBitmap([]uint16{0, 2, 4, 6})
	defer releaseContainer(sparseBitmapContainer)
	denseBitmapContainer := newContainerBitmap()
	defer releaseContainerBitmap(denseBitmapContainer)
	denseBitmapContainer.iaddRange(0, 5000)
	runContainer := buildContainerRun([]uint16{10, 11, 12, 30, 31})
	defer releaseContainer(runContainer)

	for _, tc := range []struct {
		name string
		c    container16
	}{
		{name: "Array", c: arrayContainer},
		{name: "DenseBitmap", c: denseBitmapContainer},
		{name: "Run", c: runContainer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind, meta, payload, err := bitmap32WireEncoding(tc.c)
			if err != nil {
				t.Fatalf("bitmap32WireEncoding: %v", err)
			}
			read, n, err := readBitmap32WireContainer(bytes.NewReader(payload), kind, meta)
			if err != nil {
				t.Fatalf("readBitmap32WireContainer: %v", err)
			}
			defer releaseContainer(read)
			if int(n) != len(payload) {
				t.Fatalf("read byte count mismatch: got=%d want=%d", n, len(payload))
			}
			assertSameContainerSet(t, read, containerToSlice(tc.c))
		})
	}

	if _, _, _, err := bitmap32WireEncoding(sparseBitmapContainer); err == nil {
		t.Fatalf("expected sparse bitmap container serialization to fail")
	}

	tests := []struct {
		name    string
		kind    byte
		meta    uint16
		payload []byte
		wantSub string
	}{
		{name: "InvalidKind", kind: 99, wantSub: "invalid bitmap32 container kind"},
		{name: "InvalidSparseBitmap", kind: bitmap32WireContainerBitmap, meta: arrayDefaultMaxSize - 1, wantSub: "invalid containerBitmap cardinality"},
		{name: "UnsortedArray", kind: bitmap32WireContainerArray, meta: 1, payload: []byte{3, 0, 1, 0}, wantSub: "strictly increasing"},
		{name: "DuplicateArray", kind: bitmap32WireContainerArray, meta: 1, payload: []byte{3, 0, 3, 0}, wantSub: "strictly increasing"},
		{name: "BitmapCardinalityMismatch", kind: bitmap32WireContainerBitmap, meta: 5000 - 1, payload: func() []byte {
			payload := make([]byte, bitmapContainerWords*8)
			payload[0] = 1
			return payload
		}(), wantSub: "cardinality mismatch"},
		{name: "OverlappingRun", kind: bitmap32WireContainerRun, meta: 1, payload: append([]byte(nil), interval16SliceAsByteSlice([]interval16{
			{start: 10, length: 2},
			{start: 12, length: 1},
		})...), wantSub: "non-adjacent"},
		{name: "OverflowedRun", kind: bitmap32WireContainerRun, meta: 0, payload: append([]byte(nil), interval16SliceAsByteSlice([]interval16{
			{start: MaxUint16, length: 1},
		})...), wantSub: "overflows"},
		{name: "TruncatedArray", kind: bitmap32WireContainerArray, meta: 1, payload: []byte{1}, wantSub: "EOF"},
		{name: "TruncatedBitmap", kind: bitmap32WireContainerBitmap, meta: 5000 - 1, payload: make([]byte, bitmapContainerWords*8-1), wantSub: "EOF"},
		{name: "TruncatedRun", kind: bitmap32WireContainerRun, meta: 1, payload: []byte{1, 2, 3}, wantSub: "EOF"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := readBitmap32WireContainer(bytes.NewReader(tc.payload), tc.kind, tc.meta); err == nil || !bytes.Contains([]byte(err.Error()), []byte(tc.wantSub)) {
				t.Fatalf("unexpected error: %v want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestBitmap32ReadFromRejectsNonMonotonicKeys(t *testing.T) {
	payload := encodeBitmap32Payload(
		encodeBitmap32ArrayWireContainer(1, 1),
		encodeBitmap32ArrayWireContainer(0, 2),
	)

	receiver := buildBitmap32(9, 11, 13)
	defer releaseBitmap32(receiver)

	if _, err := receiver.ReadFrom(bytes.NewReader(payload)); err == nil || !bytes.Contains([]byte(err.Error()), []byte("strictly increasing")) {
		t.Fatalf("unexpected error: %v", err)
	}
	if !receiver.isEmpty() {
		t.Fatalf("receiver must be cleared on read failure")
	}
}

func TestBitmap32ReadWriteRoundTripAndReceiverReuseOnError(t *testing.T) {
	rb := newBitmap32()
	defer releaseBitmap32(rb)
	rb.addMany([]uint32{1, 3, 7, 11})
	rb.addRange(1<<16, (1<<16)+32)
	for i := uint32(0); i < 5000; i++ {
		rb.add((2 << 16) | (i << 1))
	}
	rb.runOptimize()

	var payload bytes.Buffer
	if _, err := rb.WriteTo(&payload); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if rb.serializedSizeInBytes() != uint64(payload.Len()) {
		t.Fatalf("serializedSizeInBytes mismatch: got=%d want=%d", rb.serializedSizeInBytes(), payload.Len())
	}

	var got bitmap32
	if _, err := got.ReadFrom(bytes.NewReader(payload.Bytes())); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	defer releaseBitmap32(&got)
	assertSameBitmap32Set(t, &got, bitmap32ToSlice(rb))

	corrupted := slices.Clone(payload.Bytes())
	binary.LittleEndian.PutUint32(corrupted[:4], maxCapacity+1)
	receiver := buildBitmap32(9, 11, 13)
	defer releaseBitmap32(receiver)
	if _, err := receiver.ReadFrom(bytes.NewReader(corrupted)); err == nil {
		t.Fatalf("expected read error")
	}
	assertSameBitmap32Set(t, receiver, []uint32{9, 11, 13})
}

func TestBitmap32SharedCloneDetachWritableContainer(t *testing.T) {
	src := newBitmap32()
	src.addRange(0, 512)
	src.addRange(1<<16, (1<<16)+16)
	src.runOptimize()

	shared := src.cloneSharedInto(newBitmap32())
	defer releaseBitmap32(shared)

	original := shared.highlowcontainer.getContainerAtIndex(0)
	writable := shared.highlowcontainer.getWritableContainerAtIndex(0)
	if writable == original {
		t.Fatalf("shared bitmap32 did not detach writable container")
	}
	writable.iadd(777)
	if src.contains(777) {
		t.Fatalf("detached writable container mutated source")
	}

	retained := retainBitmap32(src)
	releaseBitmap32(src)
	if !retained.contains(1) {
		t.Fatalf("retained bitmap lost data after source release")
	}
	releaseBitmap32(retained)
}

func TestBitmap32OrInterleavedSharedCloneKeepsSource(t *testing.T) {
	src := newBitmap32()
	defer releaseBitmap32(src)
	right := newBitmap32()
	defer releaseBitmap32(right)

	leftIDs := make([]uint32, 0, 96)
	rightIDs := make([]uint32, 0, 96)
	for i := uint32(0); i < 48; i++ {
		evenKey := i << 1
		evenValue := evenKey<<16 | (i + 1)
		src.add(evenValue)
		leftIDs = append(leftIDs, evenValue)

		oddKey := evenKey + 1
		rightValue := oddKey<<16 | (i + 3)
		right.add(rightValue)
		rightIDs = append(rightIDs, rightValue)

		if i%6 == 0 {
			leftShared := oddKey<<16 | (i + 11)
			rightShared := oddKey<<16 | (i + 17)
			src.add(leftShared)
			right.add(rightShared)
			leftIDs = append(leftIDs, leftShared)
			rightIDs = append(rightIDs, rightShared)
		}
	}

	dst := src.cloneSharedInto(newBitmap32())
	defer releaseBitmap32(dst)
	dst.or(right)

	assertSameBitmap32Set(t, dst, unionUint32(leftIDs, rightIDs))
	assertSameBitmap32Set(t, src, leftIDs)
}

func TestBitmap32XorInterleavedSharedCloneKeepsSource(t *testing.T) {
	src := newBitmap32()
	defer releaseBitmap32(src)
	right := newBitmap32()
	defer releaseBitmap32(right)

	leftIDs := make([]uint32, 0, 128)
	rightIDs := make([]uint32, 0, 128)
	for i := uint32(0); i < 48; i++ {
		evenKey := i << 1
		evenValue := evenKey<<16 | (i + 1)
		src.add(evenValue)
		leftIDs = append(leftIDs, evenValue)

		oddKey := evenKey + 1
		switch i % 3 {
		case 0:
			v1 := oddKey<<16 | 3
			v2 := oddKey<<16 | 9
			src.add(v1)
			src.add(v2)
			right.add(v1)
			right.add(v2)
			leftIDs = append(leftIDs, v1, v2)
			rightIDs = append(rightIDs, v1, v2)
		case 1:
			leftOnly := oddKey<<16 | 5
			rightOnly := oddKey<<16 | 11
			src.add(leftOnly)
			right.add(rightOnly)
			leftIDs = append(leftIDs, leftOnly)
			rightIDs = append(rightIDs, rightOnly)
		default:
			rightOnly := oddKey<<16 | (i + 7)
			right.add(rightOnly)
			rightIDs = append(rightIDs, rightOnly)
		}
	}

	dst := src.cloneSharedInto(newBitmap32())
	defer releaseBitmap32(dst)
	dst.xor(right)

	assertSameBitmap32Set(t, dst, xorUint32(leftIDs, rightIDs))
	assertSameBitmap32Set(t, src, leftIDs)
}

func TestBitmap32ConcurrentIteratorCreateReleaseStable(t *testing.T) {
	rb := newBitmap32()
	defer releaseBitmap32(rb)
	rb.addRange(0, 1024)
	rb.addRange(1<<16, (1<<16)+256)
	for i := uint32(0); i < 5000; i++ {
		rb.add((2 << 16) | (i << 1))
	}
	rb.runOptimize()

	want := bitmap32ToSlice(rb)
	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				it := rb.iterator()
				got := make([]uint32, 0, len(want))
				for it.hasNext() {
					got = append(got, it.next())
				}
				releaseIterator(it)
				if !slices.Equal(got, want) {
					setFailed("iterator mismatch under concurrent create/release")
					return
				}

				many := rb.manyIterator()
				buf := make([]uint32, 257)
				got = got[:0]
				for {
					n := many.nextMany(buf)
					if n == 0 {
						break
					}
					got = append(got, buf[:n]...)
				}
				releaseManyIterator(many)
				if !slices.Equal(got, want) {
					setFailed("many iterator mismatch under concurrent create/release")
					return
				}
			}
		}()
	}
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}
}
