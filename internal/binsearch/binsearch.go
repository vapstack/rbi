package binsearch

func Int(s []uint, v uint) (int, bool) {
	l := len(s)
	i, j := 0, l
	for i < j {
		h := int(uint(i+j) >> 1)
		if s[h] < v {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < l && s[i] == v
}

func Int64(s []int64, v int64) (int, bool) {
	l := len(s)
	i, j := 0, l
	for i < j {
		h := int(uint(i+j) >> 1)
		if s[h] < v {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < l && s[i] == v
}

func Int32(s []int32, v int32) (int, bool) {
	l := len(s)
	i, j := 0, l
	for i < j {
		h := int(uint(i+j) >> 1)
		if s[h] < v {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < l && s[i] == v
}

func Uint(s []uint, v uint) (int, bool) {
	l := len(s)
	i, j := 0, l
	for i < j {
		h := int(uint(i+j) >> 1)
		if s[h] < v {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < l && s[i] == v
}

func Uint64(s []uint64, v uint64) (int, bool) {
	l := len(s)
	i, j := 0, l
	for i < j {
		h := int(uint(i+j) >> 1)
		if s[h] < v {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < l && s[i] == v
}

func Uint32(s []uint32, v uint32) (int, bool) {
	l := len(s)
	i, j := 0, l
	for i < j {
		h := int(uint(i+j) >> 1)
		if s[h] < v {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < l && s[i] == v
}

func String(s []string, v string) (int, bool) {
	l := len(s)
	i, j := 0, l
	for i < j {
		h := int(uint(i+j) >> 1)
		if s[h] < v {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < l && s[i] == v
}

func Float32(s []float32, v float32) (int, bool) {
	l := len(s)
	i, j := 0, l
	for i < j {
		h := int(uint(i+j) >> 1)
		x := s[h]
		if x < v || ((x != x) && !(v != v)) {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < l && s[i] == v
}

func Float64(s []float64, v float64) (int, bool) {
	l := len(s)
	i, j := 0, l
	for i < j {
		h := int(uint(i+j) >> 1)
		x := s[h]
		if x < v || ((x != x) && !(v != v)) {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < l && s[i] == v
}
