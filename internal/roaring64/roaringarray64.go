package roaring64

import "github.com/vapstack/rbi/internal/roaring64/roaring"

type roaringArray64 struct {
	keys       []uint32
	containers []*roaring.Bitmap
}

// runOptimize compresses the element containers to minimize space consumed.
func (ra *roaringArray64) runOptimize() {
	for i := range ra.containers {
		ra.containers[i].RunOptimize()
	}
}

func (ra *roaringArray64) appendContainer(key uint32, value *roaring.Bitmap) {
	ra.keys = append(ra.keys, key)
	ra.containers = append(ra.containers, value)
}

func (ra *roaringArray64) appendCopy(sa roaringArray64, startingindex int) {
	ra.appendContainer(sa.keys[startingindex], sa.containers[startingindex].Clone())
}

func (ra *roaringArray64) appendCopyMany(sa roaringArray64, startingindex, end int) {
	for i := startingindex; i < end; i++ {
		ra.appendCopy(sa, i)
	}
}

func (ra *roaringArray64) resize(newsize int) {
	for k := newsize; k < len(ra.containers); k++ {
		ra.keys[k] = 0
		ra.containers[k] = nil
	}

	ra.keys = ra.keys[:newsize]
	ra.containers = ra.containers[:newsize]
}

func (ra *roaringArray64) clear() {
	ra.releaseBitmapsInRange(0)
	ra.resize(0)
}

func (ra *roaringArray64) clearContainerAtIndex(i int) {
	ra.keys[i] = 0
	ra.containers[i] = nil
}

func (ra *roaringArray64) releaseBitmapsInRange(start int) {
	if start < 0 {
		start = 0
	}
	for i := start; i < len(ra.containers); i++ {
		if c := ra.containers[i]; c != nil {
			c.Clear()
			ra.clearContainerAtIndex(i)
		}
	}
}

func (ra *roaringArray64) serializedSizeInBytes() uint64 {
	answer := uint64(8)
	for _, c := range ra.containers {
		answer += 4
		answer += c.GetSerializedSizeInBytes()
	}
	return answer
}

func (ra *roaringArray64) clone() *roaringArray64 {
	sa := roaringArray64{}
	sa.keys = make([]uint32, len(ra.keys))
	copy(sa.keys, ra.keys)

	sa.containers = make([]*roaring.Bitmap, len(ra.containers))
	for i := range sa.containers {
		sa.containers[i] = ra.containers[i].Clone()
	}

	return &sa
}

func (ra *roaringArray64) getContainer(x uint32) *roaring.Bitmap {
	i := ra.binarySearch(0, int64(len(ra.keys)), x)
	if i < 0 {
		return nil
	}
	return ra.containers[i]
}

func (ra *roaringArray64) getContainerAtIndex(i int) *roaring.Bitmap {
	return ra.containers[i]
}

func (ra *roaringArray64) getWritableContainerAtIndex(i int) *roaring.Bitmap {
	return ra.containers[i]
}

func (ra *roaringArray64) getIndex(x uint32) int {
	size := len(ra.keys)
	if size == 0 || ra.keys[size-1] == x {
		return size - 1
	}
	return ra.binarySearch(0, int64(size), x)
}

func (ra *roaringArray64) getKeyAtIndex(i int) uint32 {
	return ra.keys[i]
}

func (ra *roaringArray64) insertNewKeyValueAt(i int, key uint32, value *roaring.Bitmap) {
	ra.keys = append(ra.keys, 0)
	ra.containers = append(ra.containers, nil)

	copy(ra.keys[i+1:], ra.keys[i:])
	copy(ra.containers[i+1:], ra.containers[i:])

	ra.keys[i] = key
	ra.containers[i] = value
}

func (ra *roaringArray64) removeAtIndex(i int) {
	removed := ra.containers[i]
	last := len(ra.keys) - 1
	copy(ra.keys[i:], ra.keys[i+1:])
	copy(ra.containers[i:], ra.containers[i+1:])
	ra.keys[last] = 0
	ra.containers[last] = nil
	ra.keys = ra.keys[:last]
	ra.containers = ra.containers[:last]
	if removed != nil {
		removed.Clear()
	}
}

func (ra *roaringArray64) setContainerAtIndex(i int, c *roaring.Bitmap) {
	if old := ra.containers[i]; old != nil && old != c {
		old.Clear()
	}
	ra.containers[i] = c
}

func (ra *roaringArray64) replaceKeyAndContainerAtIndex(i int, key uint32, c *roaring.Bitmap) {
	ra.keys[i] = key
	if old := ra.containers[i]; old != nil && old != c {
		old.Clear()
	}
	ra.containers[i] = c
}

func (ra *roaringArray64) size() int {
	return len(ra.keys)
}

func (ra *roaringArray64) binarySearch(begin, end int64, ikey uint32) int {
	low := begin
	high := end - 1
	for low+16 <= high {
		middleIndex := low + (high-low)/2
		middleValue := ra.keys[middleIndex]

		if middleValue < ikey {
			low = middleIndex + 1
		} else if middleValue > ikey {
			high = middleIndex - 1
		} else {
			return int(middleIndex)
		}
	}
	for ; low <= high; low++ {
		val := ra.keys[low]
		if val >= ikey {
			if val == ikey {
				return int(low)
			}
			break
		}
	}
	return -int(low + 1)
}

func (ra *roaringArray64) equals(o interface{}) bool {
	srb, ok := o.(roaringArray64)
	if !ok {
		return false
	}

	if srb.size() != ra.size() {
		return false
	}
	for i, k := range ra.keys {
		if k != srb.keys[i] {
			return false
		}
	}

	for i, c := range ra.containers {
		if !c.Equals(srb.containers[i]) {
			return false
		}
	}
	return true
}

/**
 * Find the smallest integer index strictly larger than pos such that array[index].key&gt;=min. If none can
 * be found, return size. Based on code by O. Kaser.
 *
 * @param min minimal value
 * @param pos index to exceed
 * @return the smallest index greater than pos such that array[index].key is at least as large as
 *         min, or size if it is not possible.
 */
func (ra *roaringArray64) advanceUntil(min uint32, pos int) int {
	lower := pos + 1

	if lower >= len(ra.keys) || ra.keys[lower] >= min {
		return lower
	}

	spansize := 1

	for lower+spansize < len(ra.keys) && ra.keys[lower+spansize] < min {
		spansize *= 2
	}
	var upper int
	if lower+spansize < len(ra.keys) {
		upper = lower + spansize
	} else {
		upper = len(ra.keys) - 1
	}

	if ra.keys[upper] == min {
		return upper
	}

	if ra.keys[upper] < min {
		return len(ra.keys)
	}

	lower += spansize >> 1

	mid := 0
	for lower+1 != upper {
		mid = (lower + upper) >> 1
		if ra.keys[mid] == min {
			return mid
		} else if ra.keys[mid] < min {
			lower = mid
		} else {
			upper = mid
		}
	}
	return upper
}
