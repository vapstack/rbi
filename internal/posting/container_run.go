package posting

import (
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/pooled"
)

//
// Copyright (c) 2016 by the roaring authors.
// Licensed under the Apache License, Version 2.0.
//
// We derive a few lines of code from the sort.Search
// function in the golang standard library. That function
// is Copyright 2009 The Go Authors, and licensed
// under the following BSD-style license.
/*
Copyright (c) 2009 The Go Authors. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
     copyright notice, this list of conditions and the following disclaimer
     in the documentation and/or other materials provided with the
     distribution.
   * Neither the name of Google Inc. nor the names of its
     contributors may be used to endorse or promote products derived from
     this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// containerRun does run-length encoding of sets of
// uint16 integers.
type containerRun struct {
	refs atomic.Int32
	// iv is a slice of sorted, non-overlapping, non-adjacent intervals.
	iv []interval16
}

// interval16 is the internal to containerRun
// structure that maintains the individual [start, last]
// closed intervals.
type interval16 struct {
	start  uint16
	length uint16 // length minus 1
}

func newInterval16Range(start, last uint16) interval16 {
	if last < start {
		panic(fmt.Sprintf("last (%d) cannot be smaller than start (%d)", last, start))
	}

	return interval16{
		start,
		last - start,
	}
}

func insertInterval16At(iv []interval16, idx int, value interval16) []interval16 {
	iv = append(iv, interval16{})
	copy(iv[idx+1:], iv[idx:])
	iv[idx] = value
	return iv
}

func deleteInterval16At(iv []interval16, idx int) []interval16 {
	last := len(iv) - 1
	copy(iv[idx:], iv[idx+1:])
	iv[last] = interval16{}
	return iv[:last]
}

func appendMergedInterval16AndCardinality(iv []interval16, card int, next interval16) ([]interval16, int) {
	if len(iv) == 0 {
		return append(iv, next), next.runlen()
	}
	last := len(iv) - 1
	if !canMerge16(iv[last], next) {
		return append(iv, next), card + next.runlen()
	}
	prev := iv[last]
	merged := mergeInterval16s(prev, next)
	iv[last] = merged
	return iv, card - prev.runlen() + merged.runlen()
}

type runBoundaryCursor struct {
	iv      []interval16
	index   int
	atStart bool
}

func newRunBoundaryCursor(iv []interval16) runBoundaryCursor {
	return runBoundaryCursor{iv: iv, atStart: true}
}

func (c *runBoundaryCursor) hasNext() bool {
	return c.index < len(c.iv)
}

func (c *runBoundaryCursor) value() int {
	iv := c.iv[c.index]
	if c.atStart {
		return int(iv.start)
	}
	return int(iv.last()) + 1
}

func (c *runBoundaryCursor) advance() {
	if c.atStart {
		c.atStart = false
		return
	}
	c.index++
	c.atStart = true
}

func xorRunsToRun(left, right []interval16, target []interval16) ([]interval16, int) {
	target = target[:0]
	cardinality := 0
	odd := false
	segmentStart := 0

	leftCursor := newRunBoundaryCursor(left)
	rightCursor := newRunBoundaryCursor(right)

	for leftCursor.hasNext() || rightCursor.hasNext() {
		var pos int
		if !rightCursor.hasNext() || (leftCursor.hasNext() && leftCursor.value() < rightCursor.value()) {
			pos = leftCursor.value()
		} else {
			pos = rightCursor.value()
		}

		toggle := false
		for leftCursor.hasNext() && leftCursor.value() == pos {
			leftCursor.advance()
			toggle = !toggle
		}
		for rightCursor.hasNext() && rightCursor.value() == pos {
			rightCursor.advance()
			toggle = !toggle
		}
		if !toggle {
			continue
		}
		if !odd {
			odd = true
			segmentStart = pos
			continue
		}
		target = append(target, newInterval16Range(uint16(segmentStart), uint16(pos-1)))
		cardinality += pos - segmentStart
		odd = false
	}

	if odd {
		target = append(target, newInterval16Range(uint16(segmentStart), MaxUint16))
		cardinality += maxCapacity - segmentStart
	}

	return target, cardinality
}

func runArrayXorToRuns(runs []interval16, array []uint16, target []interval16) ([]interval16, int) {
	target = target[:0]
	cardinality := 0
	arrayPos := 0

	for i := range runs {
		start := int(runs[i].start)
		end := int(runs[i].last())

		for arrayPos < len(array) && int(array[arrayPos]) < start {
			value := array[arrayPos]
			target, cardinality = appendMergedInterval16AndCardinality(
				target,
				cardinality,
				newInterval16Range(value, value),
			)
			arrayPos++
		}

		cursor := start
		for arrayPos < len(array) {
			value := int(array[arrayPos])
			if value > end {
				break
			}
			if cursor < value {
				target, cardinality = appendMergedInterval16AndCardinality(
					target,
					cardinality,
					newInterval16Range(uint16(cursor), uint16(value-1)),
				)
			}
			cursor = value + 1
			arrayPos++
		}

		if cursor <= end {
			target, cardinality = appendMergedInterval16AndCardinality(
				target,
				cardinality,
				newInterval16Range(uint16(cursor), uint16(end)),
			)
		}
	}

	for ; arrayPos < len(array); arrayPos++ {
		value := array[arrayPos]
		target, cardinality = appendMergedInterval16AndCardinality(
			target,
			cardinality,
			newInterval16Range(value, value),
		)
	}

	return target, cardinality
}

// flipRunRangeInto writes src xor [firstOfRange,endx) into dst and returns the
// resulting intervals together with their cardinality.
//
// dst may alias src as long as it starts before src and the backing storage has
// one spare interval slot, which lets inot reuse writable run storage in place.
func flipRunRangeInto(dst, src []interval16, firstOfRange, endx int) ([]interval16, int) {
	lastOfRange := endx - 1
	cursor := firstOfRange
	cardinality := 0
	i := 0

	for ; i < len(src); i++ {
		iv := src[i]
		if int(iv.last()) >= firstOfRange {
			break
		}
		dst, cardinality = appendMergedInterval16AndCardinality(dst, cardinality, iv)
	}

	for ; i < len(src); i++ {
		iv := src[i]
		ivStart := int(iv.start)
		ivLast := int(iv.last())
		if ivStart >= endx {
			break
		}

		if ivStart < firstOfRange {
			dst, cardinality = appendMergedInterval16AndCardinality(
				dst,
				cardinality,
				newInterval16Range(iv.start, uint16(firstOfRange-1)),
			)
			ivStart = firstOfRange
		}

		if cursor < ivStart {
			dst, cardinality = appendMergedInterval16AndCardinality(
				dst,
				cardinality,
				newInterval16Range(uint16(cursor), uint16(ivStart-1)),
			)
		}

		if nextCursor := ivLast + 1; nextCursor > cursor {
			cursor = nextCursor
		}

		if ivLast >= endx {
			if cursor < endx {
				dst, cardinality = appendMergedInterval16AndCardinality(
					dst,
					cardinality,
					newInterval16Range(uint16(cursor), uint16(lastOfRange)),
				)
			}
			dst, cardinality = appendMergedInterval16AndCardinality(
				dst,
				cardinality,
				newInterval16Range(uint16(endx), uint16(ivLast)),
			)
			i++
			break
		}
	}

	if cursor < endx {
		dst, cardinality = appendMergedInterval16AndCardinality(
			dst,
			cardinality,
			newInterval16Range(uint16(cursor), uint16(lastOfRange)),
		)
	}

	if i < len(src) {
		dst, cardinality = appendMergedInterval16AndCardinality(dst, cardinality, src[i])
		i++
		for ; i < len(src); i++ {
			iv := src[i]
			dst = append(dst, iv)
			cardinality += iv.runlen()
		}
	}

	return dst, cardinality
}

func replaceInterval16RangeWithSingle(iv []interval16, start, end int, value interval16) []interval16 {
	if start == end {
		return insertInterval16At(iv, start, value)
	}
	iv[start] = value
	if end == start+1 {
		return iv
	}
	newLen := len(iv) - (end - start - 1)
	copy(iv[start+1:], iv[end:])
	return iv[:newLen]
}

// runlen returns the count of integers in the interval.
func (iv interval16) runlen() int {
	return int(iv.length) + 1
}

func (iv interval16) last() uint16 {
	return iv.start + iv.length
}

// newRunContainerRange makes a new container16 made of just the specified closed interval [rangeStart,rangeLast]
func newContainerRunRange(rangeStart uint16, rangeLast uint16) *containerRun {
	rc := newContainerRun()
	rc.iv = slices.Grow(rc.iv, max(1, 1))
	rc.iv = rc.iv[:1]
	rc.iv[0] = newInterval16Range(rangeStart, rangeLast)
	return rc
}

// newContainerRunFromBitmap makes a new containerRun from bc,
// somewhat efficiently. For reference, see the Java
// https://github.com/RoaringBitmap/RoaringBitmap/blob/master/src/main/java/org/roaringbitmap/RunContainer.java#L145-L192
func newContainerRunFromBitmap(bc *containerBitmap) *containerRun {
	return newContainerRunFromBitmapWithRuns(bc, bc.numberOfRuns())
}

func newContainerRunFromBitmapWithRuns(bc *containerBitmap, nbrRuns int) *containerRun {
	rc := newContainerRun()
	rc.iv = slices.Grow(rc.iv, max(nbrRuns, nbrRuns))
	rc.iv = rc.iv[:nbrRuns]
	if nbrRuns == 0 {
		return rc
	}

	longCtr := 0            // index of current long in bitmap
	curWord := bc.bitmap[0] // its value
	runCount := 0
	for {
		// potentially multiword advance to first 1 bit
		for curWord == 0 && longCtr < len(bc.bitmap)-1 {
			longCtr++
			curWord = bc.bitmap[longCtr]
		}

		if curWord == 0 {
			// wrap up, no more runs
			return rc
		}
		localRunStart := countTrailingZeros(curWord)
		runStart := localRunStart + 64*longCtr
		// stuff 1s into number's LSBs
		curWordWith1s := curWord | (curWord - 1)

		// find the next 0, potentially in a later word
		runEnd := 0
		for curWordWith1s == maxWord && longCtr < len(bc.bitmap)-1 {
			longCtr++
			curWordWith1s = bc.bitmap[longCtr]
		}

		if curWordWith1s == maxWord {
			// a final unterminated run of 1s
			runEnd = wordSizeInBits + longCtr*64
			rc.iv[runCount].start = uint16(runStart)
			rc.iv[runCount].length = uint16(runEnd) - uint16(runStart) - 1
			return rc
		}
		localRunEnd := countTrailingZeros(^curWordWith1s)
		runEnd = localRunEnd + longCtr*64
		rc.iv[runCount].start = uint16(runStart)
		rc.iv[runCount].length = uint16(runEnd) - 1 - uint16(runStart)
		runCount++
		// now, zero out everything right of runEnd.
		curWord = curWordWith1s & (curWordWith1s + 1)
		// We've lathered and rinsed, so repeat...
	}
}

// newContainerRunFromArray populates a new
// containerRun from the contents of arr.
func newContainerRunFromArray(arr *containerArray) *containerRun {
	return newContainerRunFromArrayWithRuns(arr, arr.numberOfRuns())
}

func newContainerRunFromArrayWithRuns(arr *containerArray, numRuns int) *containerRun {
	rc := newContainerRun()
	rc.iv = slices.Grow(rc.iv, max(numRuns, 0))
	rc.iv = rc.iv[:0]
	n := arr.getCardinality()
	if n == 0 {
		return rc
	}

	runStart := arr.content[0]
	prev := runStart
	for i := 1; i < n; i++ {
		cur := arr.content[i]
		if cur == prev+1 {
			prev = cur
			continue
		}
		if cur < prev {
			panic(fmt.Sprintf("containerRun saw unsorted values; vals[%v]=cur=%v < prev=%v", i, cur, prev))
		}
		if cur != prev {
			rc.iv = append(rc.iv, newInterval16Range(runStart, prev))
			runStart = cur
		}
		prev = cur
	}
	rc.iv = append(rc.iv, newInterval16Range(runStart, prev))
	return rc
}

// canMerge returns true iff the intervals
// a and b either overlap or they are
// contiguous and so can be merged into
// a single interval.
func canMerge16(a, b interval16) bool {
	if int(a.last())+1 < int(b.start) {
		return false
	}
	return int(b.last())+1 >= int(a.start)
}

// haveOverlap differs from canMerge in that
// it tells you if the intersection of a
// and b would contain an element (otherwise
// it would be the empty set, and we return
// false).
func haveOverlap16(a, b interval16) bool {
	if int(a.last())+1 <= int(b.start) {
		return false
	}
	return int(b.last())+1 > int(a.start)
}

// mergeInterval16s joins a and b into a
// new interval, and panics if it cannot.
func mergeInterval16s(a, b interval16) (res interval16) {
	if !canMerge16(a, b) {
		panic(fmt.Sprintf("cannot merge %#v and %#v", a, b))
	}

	if b.start < a.start {
		res.start = b.start
	} else {
		res.start = a.start
	}

	if b.last() > a.last() {
		res.length = b.last() - res.start
	} else {
		res.length = a.last() - res.start
	}

	return
}

// intersectInterval16s returns the intersection
// of a and b. The isEmpty flag will be true if
// a and b were disjoint.
func intersectInterval16s(a, b interval16) (res interval16, isEmpty bool) {
	if !haveOverlap16(a, b) {
		isEmpty = true
		return
	}
	if b.start > a.start {
		res.start = b.start
	} else {
		res.start = a.start
	}

	bEnd := b.last()
	aEnd := a.last()
	var resEnd uint16

	if bEnd < aEnd {
		resEnd = bEnd
	} else {
		resEnd = aEnd
	}
	res.length = resEnd - res.start
	return
}

// union merges two runContainer16s, producing
// a new containerRun with the union of rc and b.
func (rc *containerRun) union(b *containerRun) *containerRun {
	// rc is also known as 'a' here, but golint insisted we
	// call it rc for consistency with the rest of the methods.

	result := newContainerRun()
	result.iv = slices.Grow(result.iv, max(len(rc.iv)+len(b.iv), 0))
	result.iv = result.iv[:0]
	m := result.iv[:0]

	alim := len(rc.iv)
	blim := len(b.iv)

	var na int // next from a
	var nb int // next from b

	// merged holds the current merge output, which might
	// get additional merges before being appended to m.
	var merged interval16
	var mergedUsed bool // is merged being used at the moment?

	var cura interval16 // currently considering this interval16 from a
	var curb interval16 // currently considering this interval16 from b

	pass := 0
	for na < alim && nb < blim {
		pass++
		cura = rc.iv[na]
		curb = b.iv[nb]

		if mergedUsed {
			mergedUpdated := false
			if canMerge16(cura, merged) {
				merged = mergeInterval16s(cura, merged)
				na = rc.indexOfIntervalAtOrAfter(int(merged.last())+1, na+1)
				mergedUpdated = true
			}
			if canMerge16(curb, merged) {
				merged = mergeInterval16s(curb, merged)
				nb = b.indexOfIntervalAtOrAfter(int(merged.last())+1, nb+1)
				mergedUpdated = true
			}
			if !mergedUpdated {
				// we know that merged is disjoint from cura and curb
				m = append(m, merged)
				mergedUsed = false
			}
			continue

		} else {
			// !mergedUsed
			if !canMerge16(cura, curb) {
				if cura.start < curb.start {
					m = append(m, cura)
					na++
				} else {
					m = append(m, curb)
					nb++
				}
			} else {
				merged = mergeInterval16s(cura, curb)
				mergedUsed = true
				na = rc.indexOfIntervalAtOrAfter(int(merged.last())+1, na+1)
				nb = b.indexOfIntervalAtOrAfter(int(merged.last())+1, nb+1)
			}
		}
	}
	var aDone, bDone bool
	if na >= alim {
		aDone = true
	}
	if nb >= blim {
		bDone = true
	}
	// finish by merging anything remaining into merged we can:
	if mergedUsed {
		if !aDone {
		aAdds:
			for na < alim {
				cura = rc.iv[na]
				if canMerge16(cura, merged) {
					merged = mergeInterval16s(cura, merged)
					na = rc.indexOfIntervalAtOrAfter(int(merged.last())+1, na+1)
				} else {
					break aAdds
				}
			}
		}

		if !bDone {
		bAdds:
			for nb < blim {
				curb = b.iv[nb]
				if canMerge16(curb, merged) {
					merged = mergeInterval16s(curb, merged)
					nb = b.indexOfIntervalAtOrAfter(int(merged.last())+1, nb+1)
				} else {
					break bAdds
				}
			}
		}

		m = append(m, merged)
	}
	if na < alim {
		m = append(m, rc.iv[na:]...)
	}
	if nb < blim {
		m = append(m, b.iv[nb:]...)
	}

	result.iv = m
	return result
}

// indexOfIntervalAtOrAfter is a helper for union.
func (rc *containerRun) indexOfIntervalAtOrAfter(key int, startIndex int) int {
	w, already, _ := rc.searchRange(key, startIndex, 0)
	if already {
		return w
	}
	return w + 1
}

// intersect returns a new containerRun holding the
// intersection of rc (also known as 'a')  and b.
func (rc *containerRun) intersect(b *containerRun) *containerRun {
	a := rc
	numa := len(a.iv)
	numb := len(b.iv)
	if numa == 0 || numb == 0 {
		return newContainerRun()
	}

	if numa == 1 && numb == 1 {
		if !haveOverlap16(a.iv[0], b.iv[0]) {
			return newContainerRun()
		}
	}

	var output *containerRun

	var acuri int
	var bcuri int

	astart := int(a.iv[acuri].start)
	bstart := int(b.iv[bcuri].start)

	var intersection interval16
	var leftoverstart int
	var isOverlap, isLeftoverA, isLeftoverB bool
	var done bool
toploop:
	for acuri < numa && bcuri < numb {

		isOverlap, isLeftoverA, isLeftoverB, leftoverstart, intersection = intersectWithLeftover16(astart, int(a.iv[acuri].last()), bstart, int(b.iv[bcuri].last()))

		if !isOverlap {
			switch {
			case astart < bstart:
				acuri, done = a.findNextIntervalThatIntersectsStartingFrom(acuri+1, bstart)
				if done {
					break toploop
				}
				astart = int(a.iv[acuri].start)

			case astart > bstart:
				bcuri, done = b.findNextIntervalThatIntersectsStartingFrom(bcuri+1, astart)
				if done {
					break toploop
				}
				bstart = int(b.iv[bcuri].start)
			}
		} else {
			// isOverlap
			if output == nil {
				// One interval can intersect multiple intervals on the other side,
				// so min(numa, numb) is not a safe bound here.
				output = newContainerRun()
				output.iv = slices.Grow(output.iv, max(numa+numb-1, 0))
				output.iv = output.iv[:0]
			}
			output.iv = append(output.iv, intersection)
			switch {
			case isLeftoverA:
				// note that we change astart without advancing acuri,
				// since we need to capture any 2ndary intersections with a.iv[acuri]
				astart = leftoverstart
				bcuri++
				if bcuri >= numb {
					break toploop
				}
				bstart = int(b.iv[bcuri].start)
			case isLeftoverB:
				// note that we change bstart without advancing bcuri,
				// since we need to capture any 2ndary intersections with b.iv[bcuri]
				bstart = leftoverstart
				acuri++
				if acuri >= numa {
					break toploop
				}
				astart = int(a.iv[acuri].start)
			default:
				// neither had leftover, both completely consumed

				// advance to next a interval
				acuri++
				if acuri >= numa {
					break toploop
				}
				astart = int(a.iv[acuri].start)

				// advance to next b interval
				bcuri++
				if bcuri >= numb {
					break toploop
				}
				bstart = int(b.iv[bcuri].start)
			}
		}
	} // end for toploop

	if output == nil {
		return newContainerRun()
	}

	return output
}

// intersectCardinality returns the cardinality of  the
// intersection of rc (also known as 'a')  and b.
func (rc *containerRun) intersectCardinality(b *containerRun) int {
	answer := 0

	a := rc
	numa := len(a.iv)
	numb := len(b.iv)
	if numa == 0 || numb == 0 {
		return 0
	}

	if numa == 1 && numb == 1 {
		if !haveOverlap16(a.iv[0], b.iv[0]) {
			return 0
		}
	}

	var acuri int
	var bcuri int

	astart := int(a.iv[acuri].start)
	bstart := int(b.iv[bcuri].start)

	var intersection interval16
	var leftoverstart int
	var isOverlap, isLeftoverA, isLeftoverB bool
	var done bool
	pass := 0
toploop:
	for acuri < numa && bcuri < numb {
		pass++

		isOverlap, isLeftoverA, isLeftoverB, leftoverstart, intersection = intersectWithLeftover16(astart, int(a.iv[acuri].last()), bstart, int(b.iv[bcuri].last()))

		if !isOverlap {
			switch {
			case astart < bstart:
				acuri, done = a.findNextIntervalThatIntersectsStartingFrom(acuri+1, bstart)
				if done {
					break toploop
				}
				astart = int(a.iv[acuri].start)

			case astart > bstart:
				bcuri, done = b.findNextIntervalThatIntersectsStartingFrom(bcuri+1, astart)
				if done {
					break toploop
				}
				bstart = int(b.iv[bcuri].start)
			}
		} else {
			// isOverlap
			answer += int(intersection.last()) - int(intersection.start) + 1
			switch {
			case isLeftoverA:
				// note that we change astart without advancing acuri,
				// since we need to capture any 2ndary intersections with a.iv[acuri]
				astart = leftoverstart
				bcuri++
				if bcuri >= numb {
					break toploop
				}
				bstart = int(b.iv[bcuri].start)
			case isLeftoverB:
				// note that we change bstart without advancing bcuri,
				// since we need to capture any 2ndary intersections with b.iv[bcuri]
				bstart = leftoverstart
				acuri++
				if acuri >= numa {
					break toploop
				}
				astart = int(a.iv[acuri].start)
			default:
				// neither had leftover, both completely consumed

				// advance to next a interval
				acuri++
				if acuri >= numa {
					break toploop
				}
				astart = int(a.iv[acuri].start)

				// advance to next b interval
				bcuri++
				if bcuri >= numb {
					break toploop
				}
				bstart = int(b.iv[bcuri].start)
			}
		}
	} // end for toploop

	return answer
}

// get returns true iff key is in the container16.
func (rc *containerRun) contains(key uint16) bool {
	_, in, _ := rc.search(int(key))
	return in
}

// searchRange returns alreadyPresent to indicate if the
// key is already in one of our interval16s.
//
// If key is alreadyPresent, then whichInterval16 tells
// you where.
//
// If key is not already present, then whichInterval16 is
// set as follows:
//
//	a) whichInterval16 == len(rc.iv)-1 if key is beyond our
//	   last interval16 in rc.iv;
//
//	b) whichInterval16 == -1 if key is before our first
//	   interval16 in rc.iv;
//
//	c) whichInterval16 is set to the minimum index of rc.iv
//	   which comes strictly before the key;
//	   so  rc.iv[whichInterval16].last < key,
//	   and  if whichInterval16+1 exists, then key < rc.iv[whichInterval16+1].start
//	   (Note that whichInterval16+1 won't exist when
//	   whichInterval16 is the last interval.)
//
// containerRun.search always returns whichInterval16 < len(rc.iv).
//
// The search space is from startIndex to endxIndex. If endxIndex is set to zero, then there
// no upper bound.
func (rc *containerRun) searchRange(key int, startIndex int, endxIndex int) (whichInterval16 int, alreadyPresent bool, numCompares int) {
	n := len(rc.iv)
	if n == 0 {
		return -1, false, 0
	}
	if endxIndex == 0 {
		endxIndex = n
	}

	// sort.Search returns the smallest index i
	// in [0, n) at which f(i) is true, assuming that on the range [0, n),
	// f(i) == true implies f(i+1) == true.
	// If there is no such index, Search returns n.

	// For correctness, this began as verbatim snippet from
	// sort.Search in the Go standard lib.
	// We inline our comparison function for speed, and
	// annotate with numCompares
	// to observe and test that extra bounds are utilized.
	i, j := startIndex, endxIndex
	for i < j {
		h := i + (j-i)/2 // avoid overflow when computing h as the bisector
		// i <= h < j
		numCompares++
		if key >= int(rc.iv[h].start) {
			i = h + 1
		} else {
			j = h
		}
	}
	below := i
	// end std lib snippet.

	// The above is a simple in-lining and annotation of:
	/*	below := sort.Search(n,
		func(i int) bool {
			return key < rc.iv[i].start
		})
	*/
	whichInterval16 = below - 1

	if below == n {
		// all falses => key is >= start of all interval16s
		// ... so does it belong to the last interval16?
		if key < int(rc.iv[n-1].last())+1 {
			// yes, it belongs to the last interval16
			alreadyPresent = true
			return
		}
		// no, it is beyond the last interval16.
		// leave alreadyPreset = false
		return
	}

	// INVAR: key is below rc.iv[below]
	if below == 0 {
		// key is before the first first interval16.
		// leave alreadyPresent = false
		return
	}

	// INVAR: key is >= rc.iv[below-1].start and
	//        key is <  rc.iv[below].start

	// is key in below-1 interval16?
	if key >= int(rc.iv[below-1].start) && key < int(rc.iv[below-1].last())+1 {
		// yes, it is. key is in below-1 interval16.
		alreadyPresent = true
		return
	}

	// INVAR: key >= rc.iv[below-1].endx && key < rc.iv[below].start
	// leave alreadyPresent = false
	return
}

// search returns alreadyPresent to indicate if the
// key is already in one of our interval16s.
//
// If key is alreadyPresent, then whichInterval16 tells
// you where.
//
// If key is not already present, then whichInterval16 is
// set as follows:
//
//	a) whichInterval16 == len(rc.iv)-1 if key is beyond our
//	   last interval16 in rc.iv;
//
//	b) whichInterval16 == -1 if key is before our first
//	   interval16 in rc.iv;
//
//	c) whichInterval16 is set to the maximum index of rc.iv
//	   which comes strictly before the key;
//	   so  rc.iv[whichInterval16].last < key,
//	   and  if whichInterval16+1 exists, then key < rc.iv[whichInterval16+1].start
//	   (Note that whichInterval16+1 won't exist when
//	   whichInterval16 is the last interval.)
//
// containerRun.search always returns whichInterval16 < len(rc.iv).
func (rc *containerRun) search(key int) (whichInterval16 int, alreadyPresent bool, numCompares int) {
	return rc.searchRange(key, 0, 0)
}

// getCardinality returns the count of the integers stored in the
// containerRun. The running complexity depends on the size
// of the container16.
func (rc *containerRun) getCardinality() int {
	// have to compute it
	n := 0
	for _, p := range rc.iv {
		n += p.runlen()
	}
	return n
}

func fillArrayFromRuns(dst []uint16, iv []interval16) int {
	pos := 0
	for i := range iv {
		start := int(iv[i].start)
		end := int(iv[i].last()) + 1
		for value := start; value < end; value++ {
			dst[pos] = uint16(value)
			pos++
		}
	}
	return pos
}

// isEmpty returns true if the container16 is empty.
// It runs in constant time.
func (rc *containerRun) isEmpty() bool {
	return len(rc.iv) == 0
}

func (rc *containerRun) retain() container16 {
	rc.refs.Add(1)
	return rc
}

func (rc *containerRun) uniquelyOwned() bool {
	return rc.refs.Load() == 1
}

func (rc *containerRun) release() {
	if rc == nil {
		return
	}
	if rc.refs.Add(-1) != 0 {
		return
	}
	if cap(rc.iv) > maxPooledRunContainerCapacity {
		return
	}
	runContainerPool.Put(rc)
}

func (rc *containerRun) Release() { rc.release() }

// asSlice decompresses the contents into a []uint16 slice.
func (rc *containerRun) asSlice() []uint16 {
	s := make([]uint16, rc.getCardinality())
	fillArrayFromRuns(s, rc.iv)
	return s
}

// newContainerRun creates an empty containerRun.
func newContainerRun() *containerRun {
	return runContainerPool.Get()
}

// newContainerRunCopyIv creates a containerRun, initializing
// with a copy of the supplied iv slice.
func newContainerRunCopyIv(iv []interval16) *containerRun {
	rc := newContainerRun()
	rc.iv = slices.Grow(rc.iv, len(iv))
	rc.iv = rc.iv[:len(iv)]
	copy(rc.iv, iv)
	return rc
}

func (rc *containerRun) cloneRun() *containerRun {
	rc2 := newContainerRunCopyIv(rc.iv)
	return rc2
}

// newContainerRunTakeOwnership returns a new containerRun
// backed by the provided iv slice, which we will
// assume exclusive control over from now on.
func newContainerRunTakeOwnership(iv []interval16) *containerRun {
	rc := newContainerRun()
	rc.iv = iv
	return rc
}

const (
	baseRc16Size        = 2
	perIntervalRc16Size = 4
)

// getSizeInBytes returns the number of bytes of memory
// required by this containerRun.
func (rc *containerRun) getSizeInBytes() int {
	return perIntervalRc16Size*len(rc.iv) + baseRc16Size
}

// containerRunSerializedSizeInBytes returns the number of bytes of disk
// required to hold numRuns in a containerRun.
func containerRunSerializedSizeInBytes(numRuns int) int {
	return perIntervalRc16Size*numRuns + baseRc16Size
}

// add inserts a single value k into the set.
func (rc *containerRun) add(k uint16) (wasNew bool) {
	k64 := int(k)

	index, present, _ := rc.search(k64)
	if present {
		return // already there
	}
	wasNew = true

	n := len(rc.iv)
	if index == -1 {
		// we may need to extend the first run
		if n > 0 {
			if rc.iv[0].start == k+1 {
				rc.iv[0].start = k
				rc.iv[0].length++
				return
			}
		}
		// nope, k stands alone, starting the new first interval16.
		rc.iv = insertInterval16At(rc.iv, 0, newInterval16Range(k, k))
		return
	}

	// are we off the end? handle both index == n and index == n-1:
	if index >= n-1 {
		if int(rc.iv[n-1].last())+1 == k64 {
			rc.iv[n-1].length++
			return
		}
		rc.iv = append(rc.iv, newInterval16Range(k, k))
		return
	}

	// INVAR: index and index+1 both exist, and k goes between them.
	//
	// Now: add k into the middle,
	// possibly fusing with index or index+1 interval16
	// and possibly resulting in fusing of two interval16s
	// that had a one integer gap.

	left := index
	right := index + 1

	// are we fusing left and right by adding k?
	if int(rc.iv[left].last())+1 == k64 && int(rc.iv[right].start) == k64+1 {
		// fuse into left
		rc.iv[left].length = rc.iv[right].last() - rc.iv[left].start
		// remove redundant right
		rc.iv = deleteInterval16At(rc.iv, right)
		return
	}

	// are we an addition to left?
	if int(rc.iv[left].last())+1 == k64 {
		// yes
		rc.iv[left].length++
		return
	}

	// are we an addition to right?
	if int(rc.iv[right].start) == k64+1 {
		// yes
		rc.iv[right].start = k
		rc.iv[right].length++
		return
	}

	// k makes a standalone new interval16, inserted in the middle
	rc.iv = insertInterval16At(rc.iv, right, newInterval16Range(k, k))
	return
}

// runIterator16 advice: you must call hasNext()
// before calling next()/peekNext() to insure there are contents.
type runIterator16 struct {
	rc            *containerRun
	curIndex      int
	curPosInIndex uint16
}

var runIterator16Pool = pooled.Pointers[runIterator16]{
	Cleanup: func(it *runIterator16) {
		it.rc = nil
		it.curIndex = 0
		it.curPosInIndex = 0
	},
}

// newRunIterator returns a new empty run iterator over rc.
func (rc *containerRun) newRunIterator() *runIterator16 {
	it := runIterator16Pool.Get()
	it.rc = rc
	it.curIndex = 0
	it.curPosInIndex = 0
	return it
}

func (rc *containerRun) iterate(cb func(x uint16) bool) bool {
	iterator := runIterator16{rc, 0, 0}

	for iterator.hasNext() {
		if !cb(iterator.next()) {
			return false
		}
	}

	return true
}

// hasNext returns false if calling next will panic. It
// returns true when there is at least one more value
// available in the iteration sequence.
func (ri *runIterator16) hasNext() bool {
	return len(ri.rc.iv) > ri.curIndex+1 ||
		(len(ri.rc.iv) == ri.curIndex+1 && ri.rc.iv[ri.curIndex].length >= ri.curPosInIndex)
}

// next returns the next value in the iteration sequence.
func (ri *runIterator16) next() uint16 {
	next := ri.rc.iv[ri.curIndex].start + ri.curPosInIndex

	if ri.curPosInIndex == ri.rc.iv[ri.curIndex].length {
		ri.curPosInIndex = 0
		ri.curIndex++
	} else {
		ri.curPosInIndex++
	}

	return next
}

// peekNext returns the next value in the iteration sequence without advancing the iterator
func (ri *runIterator16) peekNext() uint16 {
	return ri.rc.iv[ri.curIndex].start + ri.curPosInIndex
}

// advanceIfNeeded advances as long as the next value is smaller than minval
func (ri *runIterator16) advanceIfNeeded(minval uint16) {
	if !ri.hasNext() || ri.peekNext() >= minval {
		return
	}

	// interval cannot be -1 because of minval > peekNext
	interval, isPresent, _ := ri.rc.searchRange(int(minval), ri.curIndex, len(ri.rc.iv))

	// if the minval is present, set the curPosIndex at the right position
	if isPresent {
		ri.curIndex = interval
		ri.curPosInIndex = minval - ri.rc.iv[ri.curIndex].start
	} else {
		// otherwise interval is set to to the minimum index of rc.iv
		// which comes strictly before the key, that's why we set the next interval
		ri.curIndex = interval + 1
		ri.curPosInIndex = 0
	}
}

func (ri *runIterator16) release() {
	runIterator16Pool.Put(ri)
}

func (rc *containerRun) newRunManyIterator() *runIterator16 {
	return rc.newRunIterator()
}

// hs are the high bits to include to avoid needing to reiterate over the buffer in NextMany
func (ri *runIterator16) nextMany(hs uint32, buf []uint32) int {
	n := 0

	if !ri.hasNext() {
		return n
	}

	// start and end are inclusive
	for n < len(buf) {
		moreVals := 0

		if ri.rc.iv[ri.curIndex].length >= ri.curPosInIndex {
			// add as many as you can from this seq
			moreVals = min(int(ri.rc.iv[ri.curIndex].length-ri.curPosInIndex)+1, len(buf)-n)
			base := uint32(ri.rc.iv[ri.curIndex].start+ri.curPosInIndex) | hs

			// allows BCE
			buf2 := buf[n : n+moreVals]
			for i := range buf2 {
				buf2[i] = base + uint32(i)
			}

			// update values
			n += moreVals
		}

		if moreVals+int(ri.curPosInIndex) > int(ri.rc.iv[ri.curIndex].length) {
			ri.curPosInIndex = 0
			ri.curIndex++

			if ri.curIndex == len(ri.rc.iv) {
				break
			}
		} else {
			ri.curPosInIndex += uint16(moreVals) // moreVals always fits in uint16
		}
	}

	return n
}

func (ri *runIterator16) nextMany64(hs uint64, buf []uint64) int {
	n := 0

	if !ri.hasNext() {
		return n
	}

	// start and end are inclusive
	for n < len(buf) {
		moreVals := 0

		if ri.rc.iv[ri.curIndex].length >= ri.curPosInIndex {
			// add as many as you can from this seq
			moreVals = min(int(ri.rc.iv[ri.curIndex].length-ri.curPosInIndex)+1, len(buf)-n)
			base := uint64(ri.rc.iv[ri.curIndex].start+ri.curPosInIndex) | hs

			// allows BCE
			buf2 := buf[n : n+moreVals]
			for i := range buf2 {
				buf2[i] = base + uint64(i)
			}

			// update values
			n += moreVals
		}

		if moreVals+int(ri.curPosInIndex) > int(ri.rc.iv[ri.curIndex].length) {
			ri.curPosInIndex = 0
			ri.curIndex++

			if ri.curIndex == len(ri.rc.iv) {
				break
			}
		} else {
			ri.curPosInIndex += uint16(moreVals) // moreVals always fits in uint16
		}
	}

	return n
}

// remove removes key from the container16.
func (rc *containerRun) removeKey(key uint16) (wasPresent bool) {
	var index int
	index, wasPresent, _ = rc.search(int(key))
	if !wasPresent {
		return // already removed, nothing to do.
	}
	pos := key - rc.iv[index].start
	rc.deleteAt(&index, &pos)
	return
}

func (rc *containerRun) deleteAt(curIndex *int, curPosInIndex *uint16) {
	ci := *curIndex
	pos := *curPosInIndex

	// are we first, last, or in the middle of our interval16?
	switch pos {
	case 0:
		if int(rc.iv[ci].length) == 0 {
			// our interval disappears
			rc.iv = deleteInterval16At(rc.iv, ci)
			// curIndex stays the same, since the delete did
			// the advance for us.
			*curPosInIndex = 0
		} else {
			rc.iv[ci].start++ // no longer overflowable
			rc.iv[ci].length--
		}
	case rc.iv[ci].length:
		// length
		rc.iv[ci].length--
		// our interval16 cannot disappear, else we would have been pos == 0, case first above.
		*curPosInIndex--
		// if we leave *curIndex alone, then Next() will work properly even after the delete.
	default:
		// middle
		// split into two, adding an interval16
		new0 := newInterval16Range(rc.iv[ci].start, rc.iv[ci].start+*curPosInIndex-1)

		new1start := int(rc.iv[ci].start+*curPosInIndex) + 1
		if new1start > MaxUint16 {
			panic("overflow?!?!")
		}
		new1 := newInterval16Range(uint16(new1start), rc.iv[ci].last())
		rc.iv[ci] = new0
		rc.iv = insertInterval16At(rc.iv, ci+1, new1)
		// update curIndex and curPosInIndex
		*curIndex++
		*curPosInIndex = 0
	}
}

func have4Overlap16(astart, alast, bstart, blast int) bool {
	if alast+1 <= bstart {
		return false
	}
	return blast+1 > astart
}

func intersectWithLeftover16(astart, alast, bstart, blast int) (isOverlap, isLeftoverA, isLeftoverB bool, leftoverstart int, intersection interval16) {
	if !have4Overlap16(astart, alast, bstart, blast) {
		return
	}
	isOverlap = true

	// do the intersection:
	if bstart > astart {
		intersection.start = uint16(bstart)
	} else {
		intersection.start = uint16(astart)
	}

	switch {
	case blast < alast:
		isLeftoverA = true
		leftoverstart = blast + 1
		intersection.length = uint16(blast) - intersection.start
	case alast < blast:
		isLeftoverB = true
		leftoverstart = alast + 1
		intersection.length = uint16(alast) - intersection.start
	default:
		// alast == blast
		intersection.length = uint16(alast) - intersection.start
	}

	return
}

func (rc *containerRun) findNextIntervalThatIntersectsStartingFrom(startIndex int, key int) (index int, done bool) {
	w, _, _ := rc.searchRange(key, startIndex, 0)
	// rc.search always returns w < len(rc.iv)
	if w < startIndex {
		// not found and comes before lower bound startIndex,
		// so just use the lower bound.
		if startIndex == len(rc.iv) {
			// also this bump up means that we are done
			return startIndex, true
		}
		return startIndex, false
	}

	return w, false
}

// helper for invert
func (rc *containerRun) invertlastInterval(origin uint16, lastIdx int) []interval16 {
	cur := rc.iv[lastIdx]
	if cur.last() == MaxUint16 {
		if cur.start == origin {
			return nil // empty container16
		}
		return []interval16{newInterval16Range(origin, cur.start-1)}
	}
	if cur.start == origin {
		return []interval16{newInterval16Range(cur.last()+1, MaxUint16)}
	}
	// invert splits
	return []interval16{
		newInterval16Range(origin, cur.start-1),
		newInterval16Range(cur.last()+1, MaxUint16),
	}
}

// invert returns a new container16 (not inplace), that is
// the inversion of rc. For each bit b in rc, the
// returned value has !b
func (rc *containerRun) invert() *containerRun {
	ni := len(rc.iv)
	switch ni {
	case 0:
		return newContainerRunRange(0, MaxUint16)
	case 1:
		cur := rc.iv[0]
		switch {
		case cur.start == 0 && cur.last() == MaxUint16:
			return newContainerRun()
		case cur.start == 0:
			result := newContainerRun()
			result.iv = slices.Grow(result.iv, max(1, 1))
			result.iv = result.iv[:1]
			result.iv[0] = newInterval16Range(cur.last()+1, MaxUint16)
			return result
		case cur.last() == MaxUint16:
			result := newContainerRun()
			result.iv = slices.Grow(result.iv, max(1, 1))
			result.iv = result.iv[:1]
			result.iv[0] = newInterval16Range(0, cur.start-1)
			return result
		default:
			result := newContainerRun()
			result.iv = slices.Grow(result.iv, max(2, 2))
			result.iv = result.iv[:2]
			result.iv[0] = newInterval16Range(0, cur.start-1)
			result.iv[1] = newInterval16Range(cur.last()+1, MaxUint16)
			return result
		}
	}
	result := newContainerRun()
	result.iv = slices.Grow(result.iv, max(ni+1, 0))
	result.iv = result.iv[:0]
	invstart := uint16(0)
	ult := ni - 1
	for i, cur := range rc.iv {
		if cur.start > invstart {
			result.iv = append(result.iv, newInterval16Range(invstart, cur.start-1))
		}
		if i == ult {
			if cur.last() < MaxUint16 {
				result.iv = append(result.iv, newInterval16Range(cur.last()+1, MaxUint16))
			}
			break
		}
		invstart = cur.last() + 1
	}
	return result
}

func (iv interval16) isSuperSetOf(b interval16) bool {
	return iv.start <= b.start && b.last() <= iv.last()
}

func (iv interval16) subtractInterval(del interval16) (left []interval16, delcount int) {
	isect, isEmpty := intersectInterval16s(iv, del)

	if isEmpty {
		return nil, 0
	}
	if del.isSuperSetOf(iv) {
		return nil, iv.runlen()
	}

	switch {
	case isect.start > iv.start && isect.last() < iv.last():
		new0 := newInterval16Range(iv.start, isect.start-1)
		new1 := newInterval16Range(isect.last()+1, iv.last())
		return []interval16{new0, new1}, isect.runlen()
	case isect.start == iv.start:
		return []interval16{newInterval16Range(isect.last()+1, iv.last())}, isect.runlen()
	default:
		return []interval16{newInterval16Range(iv.start, isect.start-1)}, isect.runlen()
	}
}

func (rc *containerRun) isubtract(del interval16) {
	// origiv := make([]interval16, len(rc.iv))
	// copy(origiv, rc.iv)

	n := len(rc.iv)
	if n == 0 {
		return // already done.
	}

	_, isEmpty := intersectInterval16s(newInterval16Range(rc.iv[0].start, rc.iv[n-1].last()), del)
	if isEmpty {
		return // done
	}

	// INVAR there is some intersection between rc and del
	istart, startAlready, _ := rc.search(int(del.start))
	ilast, lastAlready, _ := rc.search(int(del.last()))
	if istart == -1 {
		if ilast == n-1 && !lastAlready {
			rc.iv = nil
			return
		}
	}
	// some intervals will remain
	switch {
	case startAlready && lastAlready:
		res0, _ := rc.iv[istart].subtractInterval(del)

		// would overwrite values in iv b/c res0 can have len 2. so
		// write to origiv instead.
		lost := 1 + ilast - istart
		changeSize := len(res0) - lost
		newSize := len(rc.iv) + changeSize

		//	rc.iv = append(pre, caboose...)
		//	return

		if ilast != istart {
			res1, _ := rc.iv[ilast].subtractInterval(del)
			res0 = append(res0, res1...)
			changeSize = len(res0) - lost
			newSize = len(rc.iv) + changeSize
		}
		switch {
		case changeSize < 0:
			// shrink
			copy(rc.iv[istart+len(res0):], rc.iv[ilast+1:])
			copy(rc.iv[istart:istart+len(res0)], res0)
			rc.iv = rc.iv[:newSize]
			return
		case changeSize == 0:
			// stay the same
			copy(rc.iv[istart:istart+len(res0)], res0)
			return
		default:
			// changeSize > 0 is only possible when ilast == istart.
			// Hence we now know: changeSize == 1 and len(res0) == 2
			rc.iv = append(rc.iv, interval16{})
			// len(rc.iv) is correct now, no need to rc.iv = rc.iv[:newSize]

			// copy the tail into place
			copy(rc.iv[ilast+2:], rc.iv[ilast+1:])
			// copy the new item(s) into place
			copy(rc.iv[istart:istart+2], res0)
			return
		}

	case !startAlready && !lastAlready:
		// we get to discard whole intervals

		// from the search() definition:

		// if del.start is not present, then istart is
		// set as follows:
		//
		//  a) istart == n-1 if del.start is beyond our
		//     last interval16 in rc.iv;
		//
		//  b) istart == -1 if del.start is before our first
		//     interval16 in rc.iv;
		//
		//  c) istart is set to the minimum index of rc.iv
		//     which comes strictly before the del.start;
		//     so  del.start > rc.iv[istart].last,
		//     and  if istart+1 exists, then del.start < rc.iv[istart+1].startx

		// if del.last is not present, then ilast is
		// set as follows:
		//
		//  a) ilast == n-1 if del.last is beyond our
		//     last interval16 in rc.iv;
		//
		//  b) ilast == -1 if del.last is before our first
		//     interval16 in rc.iv;
		//
		//  c) ilast is set to the minimum index of rc.iv
		//     which comes strictly before the del.last;
		//     so  del.last > rc.iv[ilast].last,
		//     and  if ilast+1 exists, then del.last < rc.iv[ilast+1].start

		// INVAR: istart >= 0
		pre := rc.iv[:istart+1]
		if ilast == n-1 {
			rc.iv = pre
			return
		}
		// INVAR: ilast < n-1
		lost := ilast - istart
		changeSize := -lost
		newSize := len(rc.iv) + changeSize
		if changeSize != 0 {
			copy(rc.iv[ilast+1+changeSize:], rc.iv[ilast+1:])
		}
		rc.iv = rc.iv[:newSize]
		return

	case startAlready && !lastAlready:
		// we can only shrink or stay the same size
		// i.e. we either eliminate the whole interval,
		// or just cut off the right side.
		res0, _ := rc.iv[istart].subtractInterval(del)
		if len(res0) > 0 {
			// len(res) must be 1
			rc.iv[istart] = res0[0]
		}
		lost := 1 + (ilast - istart)
		changeSize := len(res0) - lost
		newSize := len(rc.iv) + changeSize
		if changeSize != 0 {
			copy(rc.iv[ilast+1+changeSize:], rc.iv[ilast+1:])
		}
		rc.iv = rc.iv[:newSize]
		return

	case !startAlready && lastAlready:
		// we can only shrink or stay the same size
		res1, _ := rc.iv[ilast].subtractInterval(del)
		lost := ilast - istart
		changeSize := len(res1) - lost
		newSize := len(rc.iv) + changeSize
		if changeSize != 0 {
			// move the tail first to make room for res1
			copy(rc.iv[ilast+1+changeSize:], rc.iv[ilast+1:])
		}
		copy(rc.iv[istart+1:], res1)
		rc.iv = rc.iv[:newSize]
		return
	}
}

// compute rc minus b, and return the result as a new value (not inplace).
// port of run_container_andnot from CRoaring...
// https://github.com/RoaringBitmap/CRoaring/blob/master/src/containers/run.c#L435-L496
func (rc *containerRun) andNotRunCopy(b *containerRun) *containerRun {
	if len(rc.iv) == 0 {
		return newContainerRun()
	}
	if len(b.iv) == 0 {
		return rc.cloneRun()
	}

	dst := newContainerRun()
	dst.iv = slices.Grow(dst.iv, max(len(rc.iv), 0))
	dst.iv = dst.iv[:0]
	apos := 0
	bpos := 0

	a := rc

	astart := a.iv[apos].start
	alast := a.iv[apos].last()
	bstart := b.iv[bpos].start
	blast := b.iv[bpos].last()

	alen := len(a.iv)
	blen := len(b.iv)

	for apos < alen && bpos < blen {
		switch {
		case alast < bstart:
			// output the first run
			dst.iv = append(dst.iv, newInterval16Range(astart, alast))
			apos++
			if apos < alen {
				astart = a.iv[apos].start
				alast = a.iv[apos].last()
			}
		case blast < astart:
			// exit the second run
			bpos++
			if bpos < blen {
				bstart = b.iv[bpos].start
				blast = b.iv[bpos].last()
			}
		default:
			//   a: [             ]
			//   b:            [    ]
			// alast >= bstart
			// blast >= astart
			if astart < bstart {
				dst.iv = append(dst.iv, newInterval16Range(astart, bstart-1))
			}
			if alast > blast {
				astart = blast + 1
			} else {
				apos++
				if apos < alen {
					astart = a.iv[apos].start
					alast = a.iv[apos].last()
				}
			}
		}
	}
	if apos < alen {
		dst.iv = append(dst.iv, newInterval16Range(astart, alast))
		apos++
		if apos < alen {
			dst.iv = append(dst.iv, a.iv[apos:]...)
		}
	}

	return dst
}

func (rc *containerRun) numberOfRuns() (nr int) {
	return len(rc.iv)
}

// compile time verify we meet interface requirements
var _ container16 = &containerRun{}

func (rc *containerRun) clone() container16 {
	return newContainerRunCopyIv(rc.iv)
}

func (rc *containerRun) minimum() uint16 {
	return rc.iv[0].start // assume not empty
}

func (rc *containerRun) maximum() uint16 {
	return rc.iv[len(rc.iv)-1].last() // assume not empty
}

func (rc *containerRun) isFull() bool {
	return (len(rc.iv) == 1) && ((rc.iv[0].start == 0) && (rc.iv[0].last() == MaxUint16))
}

func (rc *containerRun) and(a container16) container16 {
	if rc.isFull() {
		return a.clone()
	}
	switch c := a.(type) {
	case *containerRun:
		// Important: there is no reason to believe that the
		// result of intersecting two run containers is itself
		// a containerRun. Hence we convert to an efficient container16.
		// We only use run containers when they are efficient.
		return rc.intersect(c).toEfficientContainer()
	case *containerArray:
		return rc.andArray(c)
	case *containerBitmap:
		return rc.andBitmap(c)
	}
	panic("unsupported container16 type")
}

func (rc *containerRun) andCardinality(a container16) int {
	switch c := a.(type) {
	case *containerRun:
		return rc.intersectCardinality(c)
	case *containerArray:
		return rc.andArrayCardinality(c)
	case *containerBitmap:
		return rc.andBitmapCardinality(c)
	}
	panic("unsupported container16 type")
}

// andBitmap finds the intersection of rc and bc.
func (rc *containerRun) andBitmap(bc *containerBitmap) container16 {
	return bc.andRun(rc)
}

func (rc *containerRun) andArrayCardinality(ac *containerArray) int {
	pos := 0
	answer := 0
	maxpos := ac.getCardinality()
	if maxpos == 0 {
		return 0 // won't happen in actual code
	}
	v := ac.content[pos]
mainloop:
	for _, p := range rc.iv {
		for v < p.start {
			pos++
			if pos == maxpos {
				break mainloop
			}
			v = ac.content[pos]
		}
		for v <= p.last() {
			answer++
			pos++
			if pos == maxpos {
				break mainloop
			}
			v = ac.content[pos]
		}
	}
	return answer
}

func (rc *containerRun) iand(a container16) container16 {
	if rc.isFull() {
		return a.clone()
	}
	switch c := a.(type) {
	case *containerRun:
		// Important: there is no reason to believe that the
		// result of intersecting two run containers is itself
		// a containerRun. Hence we convert to an efficient container16.
		// We only use run containers when they are efficient.
		return rc.inplaceIntersect(c).toEfficientContainer()
	case *containerArray:
		// inplace intersection with array is not supported
		// It is likely not very useful either.
		return rc.andArray(c)
	case *containerBitmap:
		// inplace intersection with bitmap is not supported
		// It is very difficult to do this inplace and likely not useful.
		return rc.andBitmap(c)
	}
	panic("unsupported container16 type")
}

func (rc *containerRun) inplaceIntersect(rc2 *containerRun) *containerRun {
	sect := rc.intersect(rc2)
	replaceContainerRunStorage(rc, sect)
	return rc
}

func (rc *containerRun) andArray(ac *containerArray) container16 {
	if len(rc.iv) == 0 {
		return getContainerArray()
	}

	acCardinality := ac.getCardinality()
	c := getContainerArrayWithCap(acCardinality)

	for rlePos, arrayPos := 0, 0; arrayPos < acCardinality; {
		iv := rc.iv[rlePos]
		arrayVal := ac.content[arrayPos]

		for iv.last() < arrayVal {
			rlePos++
			if rlePos == len(rc.iv) {
				return c
			}
			iv = rc.iv[rlePos]
		}

		if iv.start > arrayVal {
			arrayPos = advanceUntil(ac.content, arrayPos, len(ac.content), iv.start)
		} else {
			c.content = append(c.content, arrayVal)
			arrayPos++
		}
	}
	return c
}

func (rc *containerRun) andNot(a container16) container16 {
	switch c := a.(type) {
	case *containerArray:
		return rc.andNotArray(c)
	case *containerBitmap:
		return rc.andNotBitmap(c)
	case *containerRun:
		return rc.andNotRun(c).toEfficientContainer()
	}
	panic("unsupported container16 type")
}

func (rc *containerRun) fillLeastSignificant16bits(x []uint32, i int, mask uint32) int {
	k := i
	var val int
	for _, p := range rc.iv {
		n := p.runlen()
		for j := 0; j < n; j++ {
			val = int(p.start) + j
			x[k] = uint32(val) | mask
			k++
		}
	}
	return k
}

func (rc *containerRun) getShortIterator() shortPeekable {
	return rc.newRunIterator()
}

func (rc *containerRun) getManyIterator() manyIterable {
	return rc.newRunManyIterator()
}

// add the values in the range [firstOfRange, endx). endx
// is still abe to express 2^16 because it is an int not an uint16.
func (rc *containerRun) iaddRange(firstOfRange, endx int) container16 {
	if firstOfRange > endx {
		panic(fmt.Sprintf("invalid %v = endx > firstOfRange", endx))
	}
	if firstOfRange == endx {
		return rc
	}

	if len(rc.iv) == 0 {
		rc.iv = append(rc.iv, newInterval16Range(uint16(firstOfRange), uint16(endx-1)))
		return rc
	}

	start := rc.indexOfIntervalAtOrAfter(firstOfRange, 0)
	if start > 0 && int(rc.iv[start-1].last())+1 >= firstOfRange {
		start--
	}

	mergedStart := firstOfRange
	mergedLast := endx - 1
	end := start
	for end < len(rc.iv) && int(rc.iv[end].start) <= mergedLast+1 {
		if currentStart := int(rc.iv[end].start); currentStart < mergedStart {
			mergedStart = currentStart
		}
		if currentLast := int(rc.iv[end].last()); currentLast > mergedLast {
			mergedLast = currentLast
		}
		end++
	}

	rc.iv = replaceInterval16RangeWithSingle(
		rc.iv,
		start,
		end,
		newInterval16Range(uint16(mergedStart), uint16(mergedLast)),
	)
	return rc
}

// remove the values in the range [firstOfRange,endx)
func (rc *containerRun) iremoveRange(firstOfRange, endx int) container16 {
	if firstOfRange > endx {
		panic(fmt.Sprintf("request to iremove empty set [%v, %v),"+
			" nothing to do.", firstOfRange, endx))
	}
	// empty removal
	if firstOfRange == endx {
		return rc
	}
	x := newInterval16Range(uint16(firstOfRange), uint16(endx-1))
	rc.isubtract(x)
	return rc
}

// not flip the values in the range [firstOfRange,endx)
func (rc *containerRun) not(firstOfRange, endx int) container16 {
	if firstOfRange > endx {
		panic(fmt.Sprintf("invalid %v = endx > firstOfRange = %v", endx, firstOfRange))
	}

	return rc.notCopy(firstOfRange, endx)
}

// notCopy flips the values in the range [firstOfRange,endx).
// This is not inplace. Only the returned value has the flipped bits.
func (rc *containerRun) notCopy(firstOfRange, endx int) *containerRun {
	if firstOfRange > endx {
		panic(fmt.Sprintf("invalid %v = endx > firstOfRange == %v", endx, firstOfRange))
	}
	if firstOfRange >= endx {
		return rc.cloneRun()
	}
	result := newContainerRun()
	result.iv = slices.Grow(result.iv, max(len(rc.iv)+1, 0))
	result.iv = result.iv[:0]
	result.iv, _ = flipRunRangeInto(result.iv[:0], rc.iv, firstOfRange, endx)
	return result
}

// equals is now logical equals; it does not require the
// same underlying container16 type.
func (rc *containerRun) equals(o container16) bool {
	switch other := o.(type) {
	case *containerRun:
		// Check if the containers are the same object.
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
	case *containerArray:
		return equalArrayRun(other, rc)
	case *containerBitmap:
		if other.cardinality != rc.getCardinality() {
			return false
		}
		bit := bitmapContainerShortIterator{ptr: other, i: other.nextSetBit(0)}
		rit := runIterator16{rc: rc}
		for rit.hasNext() {
			if bit.next() != rit.next() {
				return false
			}
		}
		return true
	}
	panic("unsupported container16 type")
}

func (rc *containerRun) iaddReturnMinimized(x uint16) container16 {
	rc.add(x)
	return rc.toEfficientContainer()
}

func (rc *containerRun) iadd(x uint16) (wasNew bool) {
	return rc.add(x)
}

func (rc *containerRun) iremoveReturnMinimized(x uint16) container16 {
	rc.removeKey(x)
	return rc.toEfficientContainer()
}

func (rc *containerRun) iremove(x uint16) bool {
	return rc.removeKey(x)
}

func (rc *containerRun) or(a container16) container16 {
	if rc.isFull() {
		return rc.clone()
	}
	switch c := a.(type) {
	case *containerRun:
		return rc.union(c)
	case *containerArray:
		return rc.orArray(c)
	case *containerBitmap:
		return rc.orBitmap(c)
	}
	panic("unsupported container16 type")
}

// orBitmap finds the union of rc and bc.
func (rc *containerRun) orBitmap(bc *containerBitmap) container16 {
	answer := bc.clone().(*containerBitmap)
	for i := range rc.iv {
		answer.cardinality += setBitmapRangeAndCardinalityChange(answer.bitmap, int(rc.iv[i].start), int(rc.iv[i].last())+1)
	}
	return efficientContainerFromTempBitmap(answer)
}

func (rc *containerRun) andBitmapCardinality(bc *containerBitmap) int {
	answer := 0
	for i := range rc.iv {
		answer += bc.getCardinalityInRange(uint(rc.iv[i].start), uint(rc.iv[i].last())+1)
	}
	// bc.computeCardinality()
	return answer
}

// orArray finds the union of rc and ac.
func (rc *containerRun) orArray(ac *containerArray) container16 {
	if ac.isEmpty() {
		return rc.clone()
	}
	if rc.isEmpty() {
		return ac.clone()
	}
	result := newContainerRun()
	result.iv = slices.Grow(result.iv, max(len(rc.iv)+len(ac.content), 0))
	result.iv = result.iv[:0]
	var cardminusone uint16
	result.iv, cardminusone = runArrayUnionToRuns(rc.iv, ac, result.iv[:0])
	return efficientContainerFromTempRun(result, int(cardminusone)+1)
}

func (rc *containerRun) ior(a container16) container16 {
	if rc.isFull() {
		return rc
	}
	switch c := a.(type) {
	case *containerRun:
		return rc.inplaceUnion(c)
	case *containerArray:
		return rc.iorArray(c)
	case *containerBitmap:
		return rc.orBitmap(c)
	}
	panic("unsupported container16 type")
}

func (rc *containerRun) inplaceUnion(rc2 *containerRun) container16 {
	unioned := rc.union(rc2)
	replaceContainerRunStorage(rc, unioned)
	return rc.toEfficientContainer()
}

func (rc *containerRun) iorArray(ac *containerArray) container16 {
	if rc.isEmpty() {
		return ac.clone()
	}
	if ac.isEmpty() {
		return rc
	}
	var cardMinusOne uint16
	origLen := len(rc.iv)
	maxIntervals := origLen + len(ac.content)
	if cap(rc.iv) >= maxIntervals {
		buf := rc.iv[:maxIntervals]
		// Move unread runs to the tail so the merge can write back into the front.
		copy(buf[len(ac.content):], buf[:origLen])
		rc.iv, cardMinusOne = runArrayUnionToRuns(buf[len(ac.content):len(ac.content)+origLen], ac, buf[:0])
	} else {
		donor := newContainerRun()
		donor.iv = slices.Grow(donor.iv, max(maxIntervals, 0))
		donor.iv = donor.iv[:0]
		donor.iv, cardMinusOne = runArrayUnionToRuns(rc.iv, ac, donor.iv[:0])
		answer := donor.toEfficientContainerFromCardinality(int(cardMinusOne) + 1)
		if runrc, ok := answer.(*containerRun); ok {
			replaceContainerRunStorage(rc, runrc)
			return rc
		}
		donor.release()
		return answer
	}
	return rc.toEfficientContainerFromCardinality(int(cardMinusOne) + 1)

}

func runArrayUnionToRuns(runs []interval16, ac *containerArray, target []interval16) ([]interval16, uint16) {
	pos1 := 0
	pos2 := 0
	length1 := len(ac.content)
	length2 := len(runs)
	target = target[:0]
	// have to find the first range
	// options are
	// 1. from containerArray
	// 2. from containerRun
	var previousInterval interval16
	var cardMinusOne uint16
	if ac.content[0] < runs[0].start {
		previousInterval.start = ac.content[0]
		previousInterval.length = 0
		pos1++
	} else {
		previousInterval.start = runs[0].start
		previousInterval.length = runs[0].length
		pos2++
	}

	for pos1 < length1 || pos2 < length2 {
		if pos1 < length1 {
			s1 := ac.content[pos1]
			if s1 <= previousInterval.start+previousInterval.length {
				pos1++
				continue
			}
			if previousInterval.last() < MaxUint16 && previousInterval.last()+1 == s1 {
				previousInterval.length++
				pos1++
				continue
			}
		}
		if pos2 < length2 {
			range2 := runs[pos2]
			if range2.start <= previousInterval.last() || range2.start > 0 && range2.start-1 == previousInterval.last() {
				pos2++
				if previousInterval.last() < range2.last() {
					previousInterval.length = range2.last() - previousInterval.start
				}
				continue
			}
		}
		cardMinusOne += previousInterval.length + 1
		target = append(target, previousInterval)
		if pos2 == length2 || pos1 < length1 && ac.content[pos1] < runs[pos2].start {
			previousInterval.start = ac.content[pos1]
			previousInterval.length = 0
			pos1++
		} else {
			previousInterval = runs[pos2]
			pos2++
		}
	}
	cardMinusOne += previousInterval.length
	target = append(target, previousInterval)

	return target, cardMinusOne
}

func (rc *containerRun) intersects(a container16) bool {
	if rc.isEmpty() || a.isEmpty() {
		return false
	}
	if rc.isFull() {
		return true
	}
	switch c := a.(type) {
	case *containerRun:
		return rc.intersectsRun(c)
	case *containerArray:
		return rc.intersectsArray(c)
	case *containerBitmap:
		return rc.intersectsBitmap(c)
	}
	panic("unsupported container16 type")
}

func (rc *containerRun) intersectsRun(other *containerRun) bool {
	acuri := 0
	bcuri := 0
	for acuri < len(rc.iv) && bcuri < len(other.iv) {
		a := rc.iv[acuri]
		b := other.iv[bcuri]
		if haveOverlap16(a, b) {
			return true
		}
		if a.last() < b.start {
			acuri++
			continue
		}
		bcuri++
	}
	return false
}

func (rc *containerRun) intersectsArray(ac *containerArray) bool {
	acCardinality := ac.getCardinality()
	if acCardinality == 0 {
		return false
	}
	for rlePos, arrayPos := 0, 0; arrayPos < acCardinality; {
		iv := rc.iv[rlePos]
		arrayVal := ac.content[arrayPos]

		for iv.last() < arrayVal {
			rlePos++
			if rlePos == len(rc.iv) {
				return false
			}
			iv = rc.iv[rlePos]
		}

		if iv.start > arrayVal {
			arrayPos = advanceUntil(ac.content, arrayPos, acCardinality, iv.start)
			continue
		}

		return true
	}
	return false
}

func (rc *containerRun) intersectsBitmap(bc *containerBitmap) bool {
	const allones = ^uint64(0)

	for _, iv := range rc.iv {
		start := uint(iv.start)
		end := uint(iv.last()) + 1
		firstword := start / 64
		endword := (end - 1) / 64

		if firstword == endword {
			if bc.bitmap[firstword]&((allones<<(start%64))&(allones>>((64-end)&63))) != 0 {
				return true
			}
			continue
		}

		if bc.bitmap[firstword]&(allones<<(start%64)) != 0 {
			return true
		}
		for word := firstword + 1; word < endword; word++ {
			if bc.bitmap[word] != 0 {
				return true
			}
		}
		if bc.bitmap[endword]&(allones>>((64-end)&63)) != 0 {
			return true
		}
	}
	return false
}

func (rc *containerRun) xor(a container16) container16 {
	switch c := a.(type) {
	case *containerArray:
		return rc.xorArray(c)
	case *containerBitmap:
		return rc.xorBitmap(c)
	case *containerRun:
		return rc.xorRun(c)
	}
	panic("unsupported container16 type")
}

func (rc *containerRun) iandNot(a container16) container16 {
	switch c := a.(type) {
	case *containerArray:
		return rc.iandNotArray(c)
	case *containerBitmap:
		return rc.iandNotBitmap(c)
	case *containerRun:
		return rc.iandNotRun(c).toEfficientContainer()
	}
	panic("unsupported container16 type")
}

// flip the values in the range [firstOfRange,endx)
func (rc *containerRun) inot(firstOfRange, endx int) container16 {
	if firstOfRange > endx {
		panic(fmt.Sprintf("invalid %v = endx > firstOfRange = %v", endx, firstOfRange))
	}
	if firstOfRange >= endx {
		return rc
	}

	oldLen := len(rc.iv)
	if cap(rc.iv) >= oldLen+1 {
		rc.iv = rc.iv[:oldLen+1]
		copy(rc.iv[1:], rc.iv[:oldLen])
		var cardinality int
		rc.iv, cardinality = flipRunRangeInto(rc.iv[:0], rc.iv[1:oldLen+1], firstOfRange, endx)
		return rc.toEfficientContainerFromCardinality(cardinality)
	}

	donor := newContainerRun()
	donor.iv = slices.Grow(donor.iv, max(oldLen+1, 0))
	donor.iv = donor.iv[:0]
	var cardinality int
	donor.iv, cardinality = flipRunRangeInto(donor.iv[:0], rc.iv, firstOfRange, endx)
	answer := donor.toEfficientContainerFromCardinality(cardinality)
	if runrc, ok := answer.(*containerRun); ok {
		replaceContainerRunStorage(rc, runrc)
		return rc
	}
	donor.release()
	return answer
}

func (rc *containerRun) andNotRun(b *containerRun) container16 {
	return rc.andNotRunCopy(b)
}

func efficientContainerFromTempRun(temp *containerRun, card int) container16 {
	if temp == nil {
		return nil
	}
	result := temp.toEfficientContainerFromCardinality(card)
	if returned, ok := result.(*containerRun); !ok || returned != temp {
		temp.release()
	}
	return result
}

func efficientContainerFromTempRunAuto(temp *containerRun) container16 {
	if temp == nil {
		return nil
	}
	result := temp.toEfficientContainer()
	if returned, ok := result.(*containerRun); !ok || returned != temp {
		temp.release()
	}
	return result
}

func (rc *containerRun) andNotArrayToRun(ac *containerArray) (*containerRun, int) {
	result := newContainerRun()
	result.iv = slices.Grow(result.iv, max(len(rc.iv)+len(ac.content), 0))
	result.iv = result.iv[:0]
	target := result.iv[:0]
	cardinality := 0
	arrayPos := 0

	for i := range rc.iv {
		start := int(rc.iv[i].start)
		end := int(rc.iv[i].last())

		for arrayPos < len(ac.content) && int(ac.content[arrayPos]) < start {
			arrayPos++
		}

		cursor := start
		for arrayPos < len(ac.content) {
			value := int(ac.content[arrayPos])
			if value > end {
				break
			}
			if cursor < value {
				target = append(target, newInterval16Range(uint16(cursor), uint16(value-1)))
				cardinality += value - cursor
			}
			cursor = value + 1
			arrayPos++
		}

		if cursor <= end {
			target = append(target, newInterval16Range(uint16(cursor), uint16(end)))
			cardinality += end - cursor + 1
		}
	}

	result.iv = target
	return result, cardinality
}

func (rc *containerRun) andNotBitmapToRun(bc *containerBitmap) (*containerRun, int) {
	result := newContainerRun()
	result.iv = slices.Grow(result.iv, max(len(rc.iv), 0))
	result.iv = result.iv[:0]
	target := result.iv[:0]
	cardinality := 0

	for i := range rc.iv {
		start := int(rc.iv[i].start)
		end := int(rc.iv[i].last()) + 1
		cursor := start

		for cursor < end {
			zeroStart := bc.nextUnsetBit(uint(cursor))
			if zeroStart < 0 || zeroStart >= end {
				break
			}
			oneStart := bc.nextSetBit(uint(zeroStart))
			if oneStart < 0 || oneStart > end {
				oneStart = end
			}
			target = append(target, newInterval16Range(uint16(zeroStart), uint16(oneStart-1)))
			cardinality += oneStart - zeroStart
			cursor = oneStart
		}
	}

	result.iv = target
	return result, cardinality
}

func (rc *containerRun) andNotArray(ac *containerArray) container16 {
	if rc.isEmpty() {
		return getContainerArray()
	}
	if ac.isEmpty() {
		return rc.clone()
	}
	result, cardinality := rc.andNotArrayToRun(ac)
	return efficientContainerFromTempRun(result, cardinality)
}

func (rc *containerRun) andNotBitmap(bc *containerBitmap) container16 {
	if rc.isEmpty() {
		return getContainerArray()
	}
	if bc.isEmpty() {
		return rc.clone()
	}
	result, cardinality := rc.andNotBitmapToRun(bc)
	return efficientContainerFromTempRun(result, cardinality)
}

func (rc *containerRun) toBitmapContainer() *containerBitmap {
	return newContainerBitmapFromRun(rc)
}

func (rc *containerRun) iandNotRun(x2 *containerRun) container16 {
	if rc.isEmpty() || x2.isEmpty() {
		return rc
	}
	rc2 := rc.andNotRunCopy(x2)
	replaceContainerRunStorage(rc, rc2)
	return rc
}

func (rc *containerRun) iandNotArray(ac *containerArray) container16 {
	if rc.isEmpty() || ac.isEmpty() {
		return rc
	}
	result, cardinality := rc.andNotArrayToRun(ac)
	if cardinality == 0 {
		result.release()
		rc.iv = rc.iv[:0]
		return rc
	}
	answer := result.toEfficientContainerFromCardinality(cardinality)
	if runrc, ok := answer.(*containerRun); ok {
		replaceContainerRunStorage(rc, runrc)
		return rc
	}
	result.release()
	return answer
}

func (rc *containerRun) iandNotBitmap(bc *containerBitmap) container16 {
	if rc.isEmpty() || bc.isEmpty() {
		return rc
	}
	result, cardinality := rc.andNotBitmapToRun(bc)
	if cardinality == 0 {
		result.release()
		rc.iv = rc.iv[:0]
		return rc
	}
	answer := result.toEfficientContainerFromCardinality(cardinality)
	if runrc, ok := answer.(*containerRun); ok {
		replaceContainerRunStorage(rc, runrc)
		return rc
	}
	result.release()
	return answer
}

func (rc *containerRun) xorRun(x2 *containerRun) container16 {
	if rc.isEmpty() {
		return efficientContainerFromTempRunAuto(x2.cloneRun())
	}
	if x2.isEmpty() {
		return efficientContainerFromTempRunAuto(rc.cloneRun())
	}

	result := newContainerRun()
	result.iv = slices.Grow(result.iv, max(len(rc.iv)+len(x2.iv), 0))
	result.iv = result.iv[:0]
	var cardinality int
	result.iv, cardinality = xorRunsToRun(rc.iv, x2.iv, result.iv[:0])
	return efficientContainerFromTempRun(result, cardinality)
}

func (rc *containerRun) xorArray(ac *containerArray) container16 {
	if rc.isEmpty() {
		return ac.clone()
	}
	if ac.isEmpty() {
		return rc.clone()
	}

	result := newContainerRun()
	result.iv = slices.Grow(result.iv, max(len(rc.iv)+len(ac.content), 0))
	result.iv = result.iv[:0]
	var cardinality int
	result.iv, cardinality = runArrayXorToRuns(rc.iv, ac.content, result.iv[:0])
	return efficientContainerFromTempRun(result, cardinality)
}

func (rc *containerRun) xorBitmap(bc *containerBitmap) container16 {
	answer := bc.clone().(*containerBitmap)
	for i := range rc.iv {
		answer.cardinality += flipBitmapRangeAndCardinalityChange(answer.bitmap, int(rc.iv[i].start), int(rc.iv[i].last())+1)
	}
	return efficientContainerFromTempBitmap(answer)
}

// convert to bitmap or array *if needed*
func (rc *containerRun) toEfficientContainer() container16 {
	sizeAsRunContainer := rc.getSizeInBytes()
	sizeAsBitmapContainer := containerBitmapSizeInBytes()
	card := rc.getCardinality()
	sizeAsArrayContainer := containerArraySizeInBytes(card)
	if sizeAsRunContainer < min(sizeAsBitmapContainer, sizeAsArrayContainer) {
		return rc
	}
	if card <= arrayDefaultMaxSize {
		return rc.toArrayContainer()
	}
	bc := newContainerBitmapFromRun(rc)
	return bc
}

func (rc *containerRun) toEfficientContainerFromCardinality(card int) container16 {
	sizeAsRunContainer := rc.getSizeInBytes()
	sizeAsBitmapContainer := containerBitmapSizeInBytes()
	sizeAsArrayContainer := containerArraySizeInBytes(card)
	if sizeAsRunContainer < min(sizeAsBitmapContainer, sizeAsArrayContainer) {
		return rc
	}
	if card <= arrayDefaultMaxSize {
		return rc.toArrayContainerFromCardinality(card)
	}
	bc := newContainerBitmapFromRun(rc)
	return bc
}

func (rc *containerRun) toArrayContainer() *containerArray {
	ac := getContainerArrayWithLen(rc.getCardinality())
	fillArrayFromRuns(ac.content, rc.iv)
	return ac
}

func (rc *containerRun) toArrayContainerFromCardinality(card int) *containerArray {
	ac := getContainerArrayWithLen(card)
	fillArrayFromRuns(ac.content, rc.iv)
	return ac
}
