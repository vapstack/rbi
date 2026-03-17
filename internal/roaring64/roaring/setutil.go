package roaring

func difference(set1 []uint16, set2 []uint16, buffer []uint16) int {
	if len(set2) == 0 {
		buffer = buffer[:len(set1)]
		copy(buffer, set1)
		return len(set1)
	}
	if len(set1) == 0 {
		return 0
	}
	pos := 0
	k1 := 0
	k2 := 0
	buffer = buffer[:cap(buffer)]
	s1 := set1[k1]
	s2 := set2[k2]
	for {
		if s1 < s2 {
			buffer[pos] = s1
			pos++
			k1++
			if k1 >= len(set1) {
				break
			}
			s1 = set1[k1]
		} else if s1 == s2 {
			k1++
			k2++
			if k1 >= len(set1) {
				break
			}
			s1 = set1[k1]
			if k2 >= len(set2) {
				for ; k1 < len(set1); k1++ {
					buffer[pos] = set1[k1]
					pos++
				}
				break
			}
			s2 = set2[k2]
		} else { // if (val1>val2)
			k2++
			if k2 >= len(set2) {
				for ; k1 < len(set1); k1++ {
					buffer[pos] = set1[k1]
					pos++
				}
				break
			}
			s2 = set2[k2]
		}
	}
	return pos
}

func exclusiveUnion2by2(set1 []uint16, set2 []uint16, buffer []uint16) int {
	if 0 == len(set2) {
		buffer = buffer[:len(set1)]
		copy(buffer, set1[:])
		return len(set1)
	}
	if 0 == len(set1) {
		buffer = buffer[:len(set2)]
		copy(buffer, set2[:])
		return len(set2)
	}
	pos := 0
	k1 := 0
	k2 := 0
	s1 := set1[k1]
	s2 := set2[k2]
	buffer = buffer[:cap(buffer)]
	for {
		if s1 < s2 {
			buffer[pos] = s1
			pos++
			k1++
			if k1 >= len(set1) {
				for ; k2 < len(set2); k2++ {
					buffer[pos] = set2[k2]
					pos++
				}
				break
			}
			s1 = set1[k1]
		} else if s1 == s2 {
			k1++
			k2++
			if k1 >= len(set1) {
				for ; k2 < len(set2); k2++ {
					buffer[pos] = set2[k2]
					pos++
				}
				break
			}
			if k2 >= len(set2) {
				for ; k1 < len(set1); k1++ {
					buffer[pos] = set1[k1]
					pos++
				}
				break
			}
			s1 = set1[k1]
			s2 = set2[k2]
		} else { // if (val1>val2)
			buffer[pos] = s2
			pos++
			k2++
			if k2 >= len(set2) {
				for ; k1 < len(set1); k1++ {
					buffer[pos] = set1[k1]
					pos++
				}
				break
			}
			s2 = set2[k2]
		}
	}
	return pos
}

// union2by2Cardinality computes the cardinality of the union
func union2by2Cardinality(set1 []uint16, set2 []uint16) int {
	pos := 0
	k1 := 0
	k2 := 0
	if 0 == len(set2) {
		return len(set1)
	}
	if 0 == len(set1) {
		return len(set2)
	}
	s1 := set1[k1]
	s2 := set2[k2]
	for {
		if s1 < s2 {
			pos++
			k1++
			if k1 >= len(set1) {
				pos += len(set2) - k2
				break
			}
			s1 = set1[k1]
		} else if s1 == s2 {
			pos++
			k1++
			k2++
			if k1 >= len(set1) {
				pos += len(set2) - k2
				break
			}
			if k2 >= len(set2) {
				pos += len(set1) - k1
				break
			}
			s1 = set1[k1]
			s2 = set2[k2]
		} else { // if (set1[k1]>set2[k2])
			pos++
			k2++
			if k2 >= len(set2) {
				pos += len(set1) - k1
				break
			}
			s2 = set2[k2]
		}
	}
	return pos
}

func intersection2by2(
	set1 []uint16,
	set2 []uint16,
	buffer []uint16,
) int {
	if len(set1)*64 < len(set2) {
		return onesidedgallopingintersect2by2(set1, set2, buffer)
	} else if len(set2)*64 < len(set1) {
		return onesidedgallopingintersect2by2(set2, set1, buffer)
	} else {
		return localintersect2by2(set1, set2, buffer)
	}
}

// intersection2by2Cardinality computes the cardinality of the intersection
func intersection2by2Cardinality(
	set1 []uint16,
	set2 []uint16,
) int {
	if len(set1)*64 < len(set2) {
		return onesidedgallopingintersect2by2Cardinality(set1, set2)
	} else if len(set2)*64 < len(set1) {
		return onesidedgallopingintersect2by2Cardinality(set2, set1)
	} else {
		return localintersect2by2Cardinality(set1, set2)
	}
}

// intersects2by2 computes whether the two sets intersect
func intersects2by2(
	set1 []uint16,
	set2 []uint16,
) bool {
	// could be optimized if one set is much larger than the other one
	if (len(set1) == 0) || (len(set2) == 0) {
		return false
	}
	index1 := 0
	index2 := 0
	value1 := set1[index1]
	value2 := set2[index2]
mainwhile:
	for {

		if value2 < value1 {
			for {
				index2++
				if index2 == len(set2) {
					break mainwhile
				}
				value2 = set2[index2]
				if value2 >= value1 {
					break
				}
			}
		}
		if value1 < value2 {
			for {
				index1++
				if index1 == len(set1) {
					break mainwhile
				}
				value1 = set1[index1]
				if value1 >= value2 {
					break
				}
			}
		} else {
			// (set2[k2] == set1[k1])
			return true
		}
	}
	return false
}

func localintersect2by2(
	set1 []uint16,
	set2 []uint16,
	buffer []uint16,
) int {
	if (len(set1) == 0) || (len(set2) == 0) {
		return 0
	}
	k1 := 0
	k2 := 0
	pos := 0
	buffer = buffer[:cap(buffer)]
	s1 := set1[k1]
	s2 := set2[k2]
mainwhile:
	for {
		if s2 < s1 {
			for {
				k2++
				if k2 == len(set2) {
					break mainwhile
				}
				s2 = set2[k2]
				if s2 >= s1 {
					break
				}
			}
		}
		if s1 < s2 {
			for {
				k1++
				if k1 == len(set1) {
					break mainwhile
				}
				s1 = set1[k1]
				if s1 >= s2 {
					break
				}
			}
		} else {
			// (set2[k2] == set1[k1])
			buffer[pos] = s1
			pos++
			k1++
			if k1 == len(set1) {
				break
			}
			s1 = set1[k1]
			k2++
			if k2 == len(set2) {
				break
			}
			s2 = set2[k2]
		}
	}
	return pos
}

// / localintersect2by2Cardinality computes the cardinality of the intersection
func localintersect2by2Cardinality(
	set1 []uint16,
	set2 []uint16,
) int {
	if (len(set1) == 0) || (len(set2) == 0) {
		return 0
	}
	index1 := 0
	index2 := 0
	pos := 0
	value1 := set1[index1]
	value2 := set2[index2]
mainwhile:
	for {
		if value2 < value1 {
			for {
				index2++
				if index2 == len(set2) {
					break mainwhile
				}
				value2 = set2[index2]
				if value2 >= value1 {
					break
				}
			}
		}
		if value1 < value2 {
			for {
				index1++
				if index1 == len(set1) {
					break mainwhile
				}
				value1 = set1[index1]
				if value1 >= value2 {
					break
				}
			}
		} else {
			// (set2[k2] == set1[k1])
			pos++
			index1++
			if index1 == len(set1) {
				break
			}
			value1 = set1[index1]
			index2++
			if index2 == len(set2) {
				break
			}
			value2 = set2[index2]
		}
	}
	return pos
}

func advanceUntil(
	array []uint16,
	pos int,
	length int,
	min uint16,
) int {
	lower := pos + 1

	if lower >= length || array[lower] >= min {
		return lower
	}

	spansize := 1

	for lower+spansize < length && array[lower+spansize] < min {
		spansize *= 2
	}
	var upper int
	if lower+spansize < length {
		upper = lower + spansize
	} else {
		upper = length - 1
	}

	if array[upper] == min {
		return upper
	}

	if array[upper] < min {
		// means
		// array
		// has no
		// item
		// >= min
		// pos = array.length;
		return length
	}

	// we know that the next-smallest span was too small
	lower += (spansize >> 1)

	mid := 0
	for lower+1 != upper {
		mid = (lower + upper) >> 1
		if array[mid] == min {
			return mid
		} else if array[mid] < min {
			lower = mid
		} else {
			upper = mid
		}
	}
	return upper
}

func onesidedgallopingintersect2by2(
	smallset []uint16,
	largeset []uint16,
	buffer []uint16,
) int {
	if 0 == len(smallset) {
		return 0
	}
	buffer = buffer[:cap(buffer)]
	k1 := 0
	k2 := 0
	pos := 0
	s1 := largeset[k1]
	s2 := smallset[k2]
mainwhile:

	for {
		if s1 < s2 {
			k1 = advanceUntil(largeset, k1, len(largeset), s2)
			if k1 == len(largeset) {
				break mainwhile
			}
			s1 = largeset[k1]
		}
		if s2 < s1 {
			k2++
			if k2 == len(smallset) {
				break mainwhile
			}
			s2 = smallset[k2]
		} else {

			buffer[pos] = s2
			pos++
			k2++
			if k2 == len(smallset) {
				break
			}
			s2 = smallset[k2]
			k1 = advanceUntil(largeset, k1, len(largeset), s2)
			if k1 == len(largeset) {
				break mainwhile
			}
			s1 = largeset[k1]
		}

	}
	return pos
}

func onesidedgallopingintersect2by2Cardinality(
	smallset []uint16,
	largeset []uint16,
) int {
	if 0 == len(smallset) {
		return 0
	}
	k1 := 0
	k2 := 0
	pos := 0
	s1 := largeset[k1]
	s2 := smallset[k2]
mainwhile:

	for {
		if s1 < s2 {
			k1 = advanceUntil(largeset, k1, len(largeset), s2)
			if k1 == len(largeset) {
				break mainwhile
			}
			s1 = largeset[k1]
		}
		if s2 < s1 {
			k2++
			if k2 == len(smallset) {
				break mainwhile
			}
			s2 = smallset[k2]
		} else {

			pos++
			k2++
			if k2 == len(smallset) {
				break
			}
			s2 = smallset[k2]
			k1 = advanceUntil(largeset, k1, len(largeset), s2)
			if k1 == len(largeset) {
				break mainwhile
			}
			s1 = largeset[k1]
		}

	}
	return pos
}

func binarySearch(array []uint16, ikey uint16) int {
	low := 0
	high := len(array) - 1
	for low+16 <= high {
		middleIndex := int(uint32(low+high) >> 1)
		middleValue := array[middleIndex]
		if middleValue < ikey {
			low = middleIndex + 1
		} else if middleValue > ikey {
			high = middleIndex - 1
		} else {
			return middleIndex
		}
	}
	for ; low <= high; low++ {
		val := array[low]
		if val >= ikey {
			if val == ikey {
				return low
			}
			break
		}
	}
	return -(low + 1)
}
