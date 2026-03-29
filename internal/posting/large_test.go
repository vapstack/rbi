package posting

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

func collectLargeIterator(it *largeIterator) []uint64 {
	out := make([]uint64, 0, 16)
	for it.HasNext() {
		out = append(out, it.Next())
	}
	return out
}

func TestLargePostingCloneIntoOverwritesAndKeepsDeepCopy(t *testing.T) {
	src := newLargePosting()
	src.add(1)
	src.add((1 << 32) | 7)
	src.add((2 << 32) | 11)
	src.add((2 << 32) | 12)
	src.runOptimize()

	dst := newLargePosting()
	dst.add(99)
	dst.add((5 << 32) | 3)

	got := src.cloneInto(dst)
	if got != dst {
		t.Fatalf("CloneInto should return destination posting")
	}
	if !dst.equals(src) {
		t.Fatalf("CloneInto result differs from source")
	}

	dst.add((9 << 32) | 1)
	if src.contains((9 << 32) | 1) {
		t.Fatalf("CloneInto result shares state with source")
	}

	empty := newLargePosting()
	empty.cloneInto(dst)
	if !dst.isEmpty() {
		t.Fatalf("CloneInto should clear destination when source is empty")
	}
}

func TestLargePostingCloneIntoAliasedDestinationKeepsSourceIntact(t *testing.T) {
	src := newLargePosting()
	src.add(1)
	src.add((1 << 32) | 3)
	src.add((2 << 32) | 5)
	src.runOptimize()

	want := src.clone()

	aliasValue := *src
	dst := &aliasValue

	got := src.cloneInto(dst)
	if got != dst {
		t.Fatalf("CloneInto should return destination posting")
	}
	if !dst.equals(src) {
		t.Fatalf("CloneInto result differs from source")
	}
	if !src.equals(want) {
		t.Fatalf("CloneInto corrupted source when destination aliased it")
	}

	dst.add((9 << 32) | 7)
	if src.contains((9 << 32) | 7) {
		t.Fatalf("aliased CloneInto result still shares state with source")
	}
}

func TestLargePostingCloneSharedIntoPreservesSourceOnMutation(t *testing.T) {
	src := newLargePosting()
	src.addRange(0, 1024)
	src.add((1 << 32) | 7)
	src.add((1 << 32) | 9)
	src.add((2 << 32) | 11)
	src.runOptimize()

	want := src.clone()

	dst := newLargePosting()
	src.cloneSharedInto(dst)

	dst.add(2048)
	dst.remove(12)
	dst.remove((1 << 32) | 7)
	extra := newLargePosting()
	extra.add((2 << 32) | 12)
	dst.or(extra)
	dst.runOptimize()

	if !src.equals(want) {
		t.Fatalf("shared clone mutation changed source posting")
	}
	if dst.equals(src) {
		t.Fatalf("shared clone mutation did not change destination posting")
	}
	if src.contains(2048) || src.contains((2<<32)|12) {
		t.Fatalf("source unexpectedly contains values added to destination")
	}
	if !src.contains(12) || !src.contains((1<<32)|7) {
		t.Fatalf("source lost existing values after destination mutation")
	}
}

func TestLargePostingCloneSharedIntoAliasedDestinationKeepsSourceIntact(t *testing.T) {
	src := newLargePosting()
	src.addRange(0, 1024)
	src.add((1 << 32) | 7)
	src.add((2 << 32) | 11)
	src.runOptimize()

	want := src.clone()

	aliasValue := *src
	dst := &aliasValue

	got := src.cloneSharedInto(dst)
	if got != dst {
		t.Fatalf("CloneSharedInto should return destination posting")
	}
	if !dst.equals(src) {
		t.Fatalf("CloneSharedInto result differs from source")
	}
	if !src.equals(want) {
		t.Fatalf("CloneSharedInto corrupted source when destination aliased it")
	}

	dst.add((9 << 32) | 7)
	if src.contains((9 << 32) | 7) {
		t.Fatalf("aliased CloneSharedInto result still shares state with source")
	}
}

func TestLargePostingOrInterleavedSharedCloneKeepsSource(t *testing.T) {
	src := newLargePosting()
	defer releaseLargePosting(src)
	right := newLargePosting()
	defer releaseLargePosting(right)

	leftIDs := make([]uint64, 0, 96)
	rightIDs := make([]uint64, 0, 96)
	for i := uint64(0); i < 48; i++ {
		evenKey := i << 1
		evenValue := evenKey<<32 | (i + 1)
		src.add(evenValue)
		leftIDs = append(leftIDs, evenValue)

		oddKey := evenKey + 1
		rightValue := oddKey<<32 | (i + 3)
		right.add(rightValue)
		rightIDs = append(rightIDs, rightValue)

		if i%6 == 0 {
			leftShared := oddKey<<32 | (i + 11)
			rightShared := oddKey<<32 | (i + 17)
			src.add(leftShared)
			right.add(rightShared)
			leftIDs = append(leftIDs, leftShared)
			rightIDs = append(rightIDs, rightShared)
		}
	}

	dst := src.cloneSharedInto(newLargePosting())
	defer releaseLargePosting(dst)
	dst.or(right)

	assertSameLargePostingSet(t, dst, unionUint64(leftIDs, rightIDs))
	assertSameLargePostingSet(t, src, leftIDs)
}

func TestLargeIteratorPool_ReusedIteratorStartsFromNewPosting(t *testing.T) {
	first := newLargePosting()
	first.add(1)
	first.add(2)
	first.add(uint64(1)<<32 | 3)

	it := acquireLargeIterator(first)
	if got := it.Next(); got != 1 {
		t.Fatalf("first iterator first value: got=%d", got)
	}
	releaseLargeIterator(it)

	second := newLargePosting()
	second.add(7)
	second.add(uint64(1)<<32 | 9)

	reused := acquireLargeIterator(second)
	defer releaseLargeIterator(reused)
	if !testRaceEnabled && reused != it {
		t.Fatalf("large iterator pool did not reuse iterator")
	}
	if got := reused.peekNext(); got != 7 {
		t.Fatalf("reused iterator leaked stale head: got=%d", got)
	}

	got := collectLargeIterator(reused)
	want := []uint64{7, uint64(1)<<32 | 9}
	if !slices.Equal(got, want) {
		t.Fatalf("reused iterator mismatch: got=%v want=%v", got, want)
	}
}

func TestAddManyBatchPool_LargeThenSmallBatchProducesExactSet(t *testing.T) {
	warm := newLargePosting()
	large := make([]uint64, 0, 8192)
	for i := 0; i < 8192; i++ {
		large = append(large, uint64(1)<<32|uint64(i))
	}
	warm.addMany(large)

	small := newLargePosting()
	small.addMany([]uint64{5, 7})

	got := small.toArray()
	want := []uint64{5, 7}
	if !slices.Equal(got, want) {
		t.Fatalf("small AddMany leaked tail from pooled batch: got=%v want=%v", got, want)
	}
}

func TestLargePostingToArrayMixedContainerShapes(t *testing.T) {
	lp := newLargePosting()
	defer releaseLargePosting(lp)

	want := []uint64{1, 3, 5}
	lp.addMany(want)

	lp.addRange(1<<16, (1<<16)+8)
	for v := uint64(1 << 16); v < (1<<16)+8; v++ {
		want = append(want, v)
	}

	dense := make([]uint64, 0, arrayDefaultMaxSize+64)
	for i := 0; i < arrayDefaultMaxSize+64; i++ {
		v := uint64(2<<16 | uint32(i<<1))
		dense = append(dense, v)
	}
	lp.addMany(dense)
	want = append(want, dense...)

	extraHigh := []uint64{1<<32 | 7, 1<<32 | 9}
	lp.addMany(extraHigh)
	want = append(want, extraHigh...)

	lp.runOptimize()

	rb := lp.highlowcontainer.getContainerAtIndex(0)
	if _, ok := rb.highlowcontainer.getContainerAtIndex(0).(*containerArray); !ok {
		t.Fatalf("expected array container, got %T", rb.highlowcontainer.getContainerAtIndex(0))
	}
	if _, ok := rb.highlowcontainer.getContainerAtIndex(1).(*containerRun); !ok {
		t.Fatalf("expected run container, got %T", rb.highlowcontainer.getContainerAtIndex(1))
	}
	if _, ok := rb.highlowcontainer.getContainerAtIndex(2).(*containerBitmap); !ok {
		t.Fatalf("expected bitmap container, got %T", rb.highlowcontainer.getContainerAtIndex(2))
	}

	assertSameLargePostingSet(t, lp, want)
}

func TestLargePostingBasicsAcrossHighWords(t *testing.T) {
	lp := newLargePosting()
	defer releaseLargePosting(lp)

	ids := []uint64{
		1,
		3,
		7,
		1<<32 | 5,
		1<<32 | 9,
		2<<32 | 11,
		2<<32 | 15,
	}
	for _, id := range ids {
		lp.add(id)
	}

	assertSameLargePostingSet(t, lp, ids)
	if !lp.contains(1<<32|9) || lp.contains(1<<32|7) {
		t.Fatalf("contains mismatch across high words")
	}
	if got := lp.minimum(); got != ids[0] {
		t.Fatalf("minimum mismatch: got=%d want=%d", got, ids[0])
	}
	if got := lp.maximum(); got != ids[len(ids)-1] {
		t.Fatalf("maximum mismatch: got=%d want=%d", got, ids[len(ids)-1])
	}

	lp.remove(1<<32 | 5)
	lp.remove(1)
	lp.remove(2<<32 | 11)
	assertSameLargePostingSet(t, lp, []uint64{3, 7, 1<<32 | 9, 2<<32 | 15})

	lp.remove(2<<32 | 15)
	assertSameLargePostingSet(t, lp, []uint64{3, 7, 1<<32 | 9})
	if lp.highlowcontainer.size() != 2 {
		t.Fatalf("expected empty high-word bucket to be removed, got %d buckets", lp.highlowcontainer.size())
	}

	if !lp.checkedAdd(1<<32|21) || lp.checkedAdd(1<<32|21) {
		t.Fatalf("checkedAdd duplicate detection mismatch")
	}
	if !lp.checkedAdd(9<<32|1) || lp.checkedAdd(9<<32|1) {
		t.Fatalf("checkedAdd new-bucket duplicate detection mismatch")
	}
	assertSameLargePostingSet(t, lp, []uint64{3, 7, 1<<32 | 9, 1<<32 | 21, 9<<32 | 1})
}

func TestLargePostingAddRangeAndAddManyMatchOracle(t *testing.T) {
	lp := newLargePosting()
	defer releaseLargePosting(lp)

	lp.addRange(10, 15)
	lp.addRange((1<<32)-2, (1<<32)+3)
	lp.addRange(2<<32|5, 2<<32|9)
	lp.addRange(9, 9)

	want := []uint64{
		10, 11, 12, 13, 14,
		(1 << 32) - 2, (1 << 32) - 1, 1 << 32, 1<<32 | 1, 1<<32 | 2,
		2<<32 | 5, 2<<32 | 6, 2<<32 | 7, 2<<32 | 8,
	}
	assertSameLargePostingSet(t, lp, want)

	many := []uint64{
		1, 3, 5,
		1<<32 | 7, 1<<32 | 9,
		2<<32 | 11, 2<<32 | 13,
	}
	fromMany := newLargePosting()
	defer releaseLargePosting(fromMany)
	fromMany.addMany(many)
	assertSameLargePostingSet(t, fromMany, many)

	elementwise := newLargePosting()
	defer releaseLargePosting(elementwise)
	for _, id := range many {
		elementwise.add(id)
	}
	if !fromMany.equals(elementwise) {
		t.Fatalf("addMany result differs from elementwise add")
	}
}

func TestLargePostingAddManyHandlesUnsortedAndDuplicates(t *testing.T) {
	lp := newLargePosting()
	defer releaseLargePosting(lp)

	input := []uint64{
		2<<32 | 11, 5, 1<<32 | 7, 5,
		1, 2<<32 | 3, 1<<32 | 7, 1<<32 | 1,
	}
	lp.addMany(input)

	assertSameLargePostingSet(t, lp, []uint64{
		1, 5,
		1<<32 | 1, 1<<32 | 7,
		2<<32 | 3, 2<<32 | 11,
	})
}

func TestLargePostingAddManySortedFullInnerContainerMinimizesToRun(t *testing.T) {
	makeBatch := func(start, end int) []uint64 {
		out := make([]uint64, end-start)
		for i := range out {
			out[i] = uint64(start + i)
		}
		return out
	}

	t.Run("Fresh", func(t *testing.T) {
		lp := newLargePosting()
		defer releaseLargePosting(lp)

		want := makeBatch(0, maxCapacity)
		lp.addMany(want)
		assertSameLargePostingSet(t, lp, want)

		if got := lp.highlowcontainer.size(); got != 1 {
			t.Fatalf("unexpected large bucket count: got=%d want=1", got)
		}
		rb := lp.highlowcontainer.getContainerAtIndex(0)
		if got := rb.highlowcontainer.size(); got != 1 {
			t.Fatalf("unexpected bitmap32 container count: got=%d want=1", got)
		}
		if _, ok := rb.highlowcontainer.getContainerAtIndex(0).(*containerRun); !ok {
			t.Fatalf("fresh sorted full inner container must minimize to run, got %T", rb.highlowcontainer.getContainerAtIndex(0))
		}
	})

	t.Run("ExistingBitmap32", func(t *testing.T) {
		lp := newLargePosting()
		defer releaseLargePosting(lp)

		want := makeBatch(0, maxCapacity)
		lp.addMany(want[:arrayDefaultMaxSize+32])
		lp.addMany(want[arrayDefaultMaxSize+32:])
		assertSameLargePostingSet(t, lp, want)

		rb := lp.highlowcontainer.getContainerAtIndex(0)
		if _, ok := rb.highlowcontainer.getContainerAtIndex(0).(*containerRun); !ok {
			t.Fatalf("sorted bulk fill through existing bitmap32 must minimize to run, got %T", rb.highlowcontainer.getContainerAtIndex(0))
		}
	})
}

func TestLargePostingSetOpsScenarios(t *testing.T) {
	tests := []struct {
		name  string
		left  []uint64
		right []uint64
	}{
		{
			name:  "Disjoint",
			left:  []uint64{1, 3, 5, 7},
			right: []uint64{2 << 32, 2<<32 | 3},
		},
		{
			name:  "PartialOverlap",
			left:  []uint64{1, 3, 5, 7, 1<<32 | 9},
			right: []uint64{3, 4, 5, 1<<32 | 9, 2<<32 | 1},
		},
		{
			name:  "Subset",
			left:  []uint64{1, 3, 5, 7, 9, 11},
			right: []uint64{3, 5, 7},
		},
		{
			name:  "CrossWordOverlap",
			left:  []uint64{(1 << 32) - 1, 1 << 32, 1<<32 | 1, 2<<32 | 7},
			right: []uint64{(1 << 32) - 1, 1 << 32, 2<<32 | 8},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			left := largePostingOf(tc.left...)
			right := largePostingOf(tc.right...)
			defer releaseLargePosting(left)
			defer releaseLargePosting(right)

			wantAnd := intersectUint64(tc.left, tc.right)
			wantOr := unionUint64(tc.left, tc.right)
			wantAndNot := differenceUint64(tc.left, tc.right)

			andPosting := left.clone()
			defer releaseLargePosting(andPosting)
			andPosting.and(right)
			assertSameLargePostingSet(t, andPosting, wantAnd)

			orPosting := left.clone()
			defer releaseLargePosting(orPosting)
			orPosting.or(right)
			assertSameLargePostingSet(t, orPosting, wantOr)

			andNotPosting := left.clone()
			defer releaseLargePosting(andNotPosting)
			andNotPosting.andNot(right)
			assertSameLargePostingSet(t, andNotPosting, wantAndNot)

			if got := left.intersects(right); got != (len(wantAnd) > 0) {
				t.Fatalf("intersects mismatch: got=%v want=%v", got, len(wantAnd) > 0)
			}
			if got := left.andCardinality(right); got != uint64(len(wantAnd)) {
				t.Fatalf("andCardinality mismatch: got=%d want=%d", got, len(wantAnd))
			}
		})
	}
}

func TestLargeIteratorAdvanceAndTransitions(t *testing.T) {
	lp := largePostingOf(
		1, 4, 7,
		1<<32|3, 1<<32|8,
		2<<32|1, 2<<32|9,
	)
	defer releaseLargePosting(lp)

	it := acquireLargeIterator(lp)
	defer releaseLargeIterator(it)

	if got := it.peekNext(); got != 1 {
		t.Fatalf("peekNext mismatch: got=%d want=1", got)
	}
	it.advanceIfNeeded(6)
	if got := it.peekNext(); got != 7 {
		t.Fatalf("advance within first high word mismatch: got=%d want=7", got)
	}
	if got := it.Next(); got != 7 {
		t.Fatalf("Next mismatch after advance: got=%d want=7", got)
	}
	it.advanceIfNeeded(1<<32 | 4)
	if got := it.peekNext(); got != 1<<32|8 {
		t.Fatalf("advance across high words mismatch: got=%d want=%d", got, 1<<32|8)
	}

	got := collectLargeIterator(it)
	want := []uint64{1<<32 | 8, 2<<32 | 1, 2<<32 | 9}
	if !slices.Equal(got, want) {
		t.Fatalf("iterator mismatch: got=%v want=%v", got, want)
	}
}

func TestLargeIteratorAdvanceEdgeCasesAndIntersectsSkips(t *testing.T) {
	t.Run("IteratorAdvanceNoopAndExhaustion", func(t *testing.T) {
		lp := largePostingOf(1, 4, 7, 1<<32|1)
		defer releaseLargePosting(lp)

		it := acquireLargeIterator(lp)
		defer releaseLargeIterator(it)

		it.advanceIfNeeded(0)
		if got := it.peekNext(); got != 1 {
			t.Fatalf("advanceIfNeeded no-op mismatch: got=%d want=1", got)
		}

		it.advanceIfNeeded(8)
		if got := it.peekNext(); got != 1<<32|1 {
			t.Fatalf("advanceIfNeeded exhaustion-of-container mismatch: got=%d want=%d", got, 1<<32|1)
		}

		it.advanceIfNeeded(2 << 32)
		if it.HasNext() {
			t.Fatalf("advanceIfNeeded past end must exhaust iterator")
		}
	})

	t.Run("IntersectsAdvancePaths", func(t *testing.T) {
		tests := []struct {
			name  string
			left  []uint64
			right []uint64
			want  bool
		}{
			{
				name:  "AdvanceLeftToMatch",
				left:  []uint64{1, 1<<32 | 1, 4<<32 | 7},
				right: []uint64{4<<32 | 7},
				want:  true,
			},
			{
				name:  "AdvanceRightToMatch",
				left:  []uint64{5<<32 | 9},
				right: []uint64{1, 2<<32 | 3, 5<<32 | 9},
				want:  true,
			},
			{
				name:  "SameKeyNoIntersection",
				left:  []uint64{7<<32 | 1},
				right: []uint64{7<<32 | 2},
				want:  false,
			},
			{
				name:  "AdvanceToEndWithoutMatch",
				left:  []uint64{1, 3, 5},
				right: []uint64{8<<32 | 1},
				want:  false,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				left := largePostingOf(tc.left...)
				right := largePostingOf(tc.right...)
				defer releaseLargePosting(left)
				defer releaseLargePosting(right)
				if got := left.intersects(right); got != tc.want {
					t.Fatalf("intersects mismatch: got=%v want=%v", got, tc.want)
				}
			})
		}
	})
}

func TestLargeArrayCopySharedDetachAndAliasSafeCopy(t *testing.T) {
	src := largePostingOf(1, 3, 5, 1<<32|7, 1<<32|9)
	defer releaseLargePosting(src)

	shared := new(largeArray)
	shared.copySharedFrom(&src.highlowcontainer)
	defer func() {
		shared.releaseContainersInRange(0)
		shared.keys = shared.keys[:0]
		shared.containers = shared.containers[:0]
	}()

	original := shared.getContainerAtIndex(0)
	writable := shared.getWritableContainerAtIndex(0)
	if writable == original {
		t.Fatalf("shared largeArray did not detach writable bitmap")
	}
	writable.add(11)
	if src.contains(11) {
		t.Fatalf("detached writable bitmap mutated source")
	}

	alias := &largeArray{
		keys:       src.highlowcontainer.keys[:len(src.highlowcontainer.keys)],
		containers: src.highlowcontainer.containers[:len(src.highlowcontainer.containers)],
	}
	alias.copyFrom(&src.highlowcontainer)
	defer func() {
		alias.releaseContainersInRange(0)
		alias.keys = alias.keys[:0]
		alias.containers = alias.containers[:0]
	}()
	if !alias.equals(&src.highlowcontainer) {
		t.Fatalf("copyFrom alias result mismatch")
	}

	alias.getWritableContainerAtIndex(0).add(13)
	if src.contains(13) {
		t.Fatalf("copyFrom alias result still shares state with source")
	}
}

func TestLargeHelpersClearAndSetContainerAtIndex(t *testing.T) {
	rb := buildBitmap32(1, 3, 5)
	rb.clear()
	if !rb.isEmpty() {
		t.Fatalf("bitmap32.clear must reset bitmap")
	}
	releaseBitmap32(rb)

	la := &largeArray{
		keys:       []uint32{7},
		containers: []*bitmap32{buildBitmap32(1, 2, 3)},
	}
	old := la.containers[0]
	replacement := buildBitmap32(11, 13)
	la.setContainerAtIndex(0, replacement)
	defer la.releaseContainersInRange(0)

	if la.getContainerAtIndex(0) != replacement {
		t.Fatalf("setContainerAtIndex did not replace container")
	}
	if !old.isEmpty() {
		t.Fatalf("setContainerAtIndex must release previous container")
	}
}

func TestLargeArrayAppendCopyAndAliases(t *testing.T) {
	src := largeArray{
		keys: []uint32{1, 3, 7},
		containers: []*bitmap32{
			buildBitmap32(1, 2),
			buildBitmap32(10, 11, 12),
			buildBitmap32(20, 21, 22),
		},
	}
	defer src.releaseContainersInRange(0)

	alias := &largeArray{
		keys:       src.keys[:2],
		containers: src.containers[:2],
	}
	if !src.aliases(alias) {
		t.Fatalf("largeArray aliases must detect shared backing")
	}

	independent := &largeArray{
		keys:       []uint32{1, 3},
		containers: []*bitmap32{buildBitmap32(1, 2), buildBitmap32(10, 11, 12)},
	}
	defer independent.releaseContainersInRange(0)
	if src.aliases(independent) {
		t.Fatalf("largeArray aliases must reject distinct backing")
	}

	var dst largeArray
	dst.appendCopy(src, 0)
	dst.appendCopyMany(src, 1, 3)
	defer dst.releaseContainersInRange(0)

	if !slices.Equal(dst.keys, src.keys) {
		t.Fatalf("appendCopy/appendCopyMany keys mismatch: got=%v want=%v", dst.keys, src.keys)
	}
	for i := range src.keys {
		assertSameBitmap32Set(t, dst.getContainerAtIndex(i), bitmap32ToSlice(src.getContainerAtIndex(i)))
	}
	dst.getWritableContainerAtIndex(0).add(99)
	if src.getContainerAtIndex(0).contains(99) {
		t.Fatalf("appendCopy result still shares bitmap32 state with source")
	}
}

func TestWriteReadAndSkipLarge(t *testing.T) {
	lp := largePostingOf(
		1, 3, 5, 7,
		1<<32|9, 1<<32|11,
		2<<32|13, 2<<32|15,
	)
	defer releaseLargePosting(lp)

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writeLarge(writer, lp); err != nil {
		t.Fatalf("writeLarge: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	roundTrip, err := readLarge(bufio.NewReader(bytes.NewReader(payload.Bytes())))
	if err != nil {
		t.Fatalf("readLarge: %v", err)
	}
	defer releaseLargePosting(roundTrip)
	assertSameLargePostingSet(t, roundTrip, lp.toArray())

	if err := skipLarge(bufio.NewReader(bytes.NewReader(payload.Bytes()))); err != nil {
		t.Fatalf("skipLarge: %v", err)
	}
}

func TestWriteReadAndSkipLargeEmptyPayloads(t *testing.T) {
	t.Run("NilLargePosting", func(t *testing.T) {
		var payload bytes.Buffer
		writer := bufio.NewWriter(&payload)
		if err := writeLarge(writer, nil); err != nil {
			t.Fatalf("writeLarge(nil): %v", err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		if !bytes.Equal(payload.Bytes(), encodeUvarint(0)) {
			t.Fatalf("nil large payload mismatch: got=%v want=%v", payload.Bytes(), encodeUvarint(0))
		}

		got, err := readLarge(bufio.NewReader(bytes.NewReader(payload.Bytes())))
		if err != nil {
			t.Fatalf("readLarge(nil payload): %v", err)
		}
		defer releaseLargePosting(got)
		if !got.isEmpty() {
			t.Fatalf("readLarge(nil payload) must return empty posting")
		}
		if err := skipLarge(bufio.NewReader(bytes.NewReader(payload.Bytes()))); err != nil {
			t.Fatalf("skipLarge(nil payload): %v", err)
		}
	})

	t.Run("EmptyLargePosting", func(t *testing.T) {
		lp := newLargePosting()
		defer releaseLargePosting(lp)

		var payload bytes.Buffer
		writer := bufio.NewWriter(&payload)
		if err := writeLarge(writer, lp); err != nil {
			t.Fatalf("writeLarge(empty): %v", err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		want := append(encodeUvarint(lp.serializedSizeInBytes()), make([]byte, 8)...)
		if !bytes.Equal(payload.Bytes(), want) {
			t.Fatalf("empty large payload mismatch: got=%v want=%v", payload.Bytes(), want)
		}

		got, err := readLarge(bufio.NewReader(bytes.NewReader(payload.Bytes())))
		if err != nil {
			t.Fatalf("readLarge(empty payload): %v", err)
		}
		defer releaseLargePosting(got)
		if !got.isEmpty() {
			t.Fatalf("readLarge(empty payload) must return empty posting")
		}
	})
}

func TestReadLargeRejectsInvalidPayloads(t *testing.T) {
	lp := largePostingOf(1, 3, 5, 7, 1<<32|9)
	defer releaseLargePosting(lp)

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writeLarge(writer, lp); err != nil {
		t.Fatalf("writeLarge: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	valid := payload.Bytes()
	size, n := binary.Uvarint(valid)
	if n <= 0 {
		t.Fatalf("failed to parse large payload size")
	}

	sizeMismatch := append(encodeUvarint(size-1), valid[n:]...)
	unsortedKeys := encodeLargePayload(
		[]uint32{1, 0},
		[][]byte{
			encodeBitmap32Payload(encodeBitmap32ArrayWireContainer(0, 1)),
			encodeBitmap32Payload(encodeBitmap32ArrayWireContainer(0, 2)),
		},
	)
	emptyChild := encodeLargePayload(
		[]uint32{0},
		[][]byte{
			encodeBitmap32Payload(),
		},
	)

	tests := []struct {
		name    string
		payload []byte
		wantSub string
	}{
		{name: "SizeMismatch", payload: sizeMismatch, wantSub: "size mismatch"},
		{name: "Oversized", payload: encodeUvarint(^uint64(0)), wantSub: "overflows int64"},
		{name: "UnsortedKeys", payload: unsortedKeys, wantSub: "strictly increasing"},
		{name: "EmptyChild", payload: emptyChild, wantSub: "is empty"},
		{name: "Truncated", payload: valid[:len(valid)-1], wantSub: "EOF"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := readLarge(bufio.NewReader(bytes.NewReader(tc.payload))); err == nil || !bytes.Contains([]byte(err.Error()), []byte(tc.wantSub)) {
				t.Fatalf("unexpected error: %v want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestLargePostingConcurrentIteratorCreateReleaseStable(t *testing.T) {
	lp := newLargePosting()
	defer releaseLargePosting(lp)
	lp.addRange(0, 1024)
	lp.addRange(1<<32, (1<<32)+256)
	for i := uint64(0); i < 5000; i++ {
		lp.add(2<<32 | (i << 1))
	}
	lp.runOptimize()

	want := lp.toArray()
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
			for i := 0; i < 400; i++ {
				it := lp.iterator()
				got := collectLargeIterator(it)
				releaseLargeIterator(it)
				if !slices.Equal(got, want) {
					setFailed("large iterator mismatch under concurrent create/release")
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
