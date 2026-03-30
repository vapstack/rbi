package posting

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"slices"
	"testing"
)

type containerFactory struct {
	name  string
	build func([]uint16) container16
}

func canonicalUint64s(ids []uint64) []uint64 {
	if len(ids) == 0 {
		return nil
	}
	out := slices.Clone(ids)
	slices.Sort(out)
	return slices.Compact(out)
}

func canonicalUint32s(ids []uint32) []uint32 {
	if len(ids) == 0 {
		return nil
	}
	out := slices.Clone(ids)
	slices.Sort(out)
	return slices.Compact(out)
}

func canonicalUint16s(ids []uint16) []uint16 {
	if len(ids) == 0 {
		return nil
	}
	out := slices.Clone(ids)
	slices.Sort(out)
	return slices.Compact(out)
}

func listToSlice(list List) []uint64 {
	return list.ToArray()
}

func bitmap32ToSlice(rb *bitmap32) []uint32 {
	if rb == nil || rb.isEmpty() {
		return nil
	}
	it := rb.iterator()
	defer releaseIterator(it)

	out := make([]uint32, 0, rb.cardinality())
	for it.hasNext() {
		out = append(out, it.next())
	}
	return out
}

func containerToSlice(c container16) []uint16 {
	if c == nil || c.isEmpty() {
		return nil
	}
	out := make([]uint16, 0, c.getCardinality())
	c.iterate(func(x uint16) bool {
		out = append(out, x)
		return true
	})
	return out
}

func assertStrictlySortedUniqueU64(t *testing.T, ids []uint64) {
	t.Helper()
	if !slices.IsSorted(ids) {
		t.Fatalf("ids are not sorted: %v", ids)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i-1] == ids[i] {
			t.Fatalf("ids are not unique: %v", ids)
		}
	}
}

func assertStrictlySortedUniqueU32(t *testing.T, ids []uint32) {
	t.Helper()
	if !slices.IsSorted(ids) {
		t.Fatalf("ids are not sorted: %v", ids)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i-1] == ids[i] {
			t.Fatalf("ids are not unique: %v", ids)
		}
	}
}

func assertStrictlySortedUniqueU16(t *testing.T, ids []uint16) {
	t.Helper()
	if !slices.IsSorted(ids) {
		t.Fatalf("ids are not sorted: %v", ids)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i-1] == ids[i] {
			t.Fatalf("ids are not unique: %v", ids)
		}
	}
}

func assertSameListSet(t *testing.T, got List, want []uint64) {
	t.Helper()
	gotSlice := listToSlice(got)
	want = canonicalUint64s(want)
	assertStrictlySortedUniqueU64(t, gotSlice)
	if !slices.Equal(gotSlice, want) {
		t.Fatalf("list mismatch: got=%v want=%v", gotSlice, want)
	}
	if got.Cardinality() != uint64(len(want)) {
		t.Fatalf("cardinality mismatch: got=%d want=%d", got.Cardinality(), len(want))
	}
}

func assertSameBitmap32Set(t *testing.T, got *bitmap32, want []uint32) {
	t.Helper()
	gotSlice := bitmap32ToSlice(got)
	want = canonicalUint32s(want)
	assertStrictlySortedUniqueU32(t, gotSlice)
	if !slices.Equal(gotSlice, want) {
		t.Fatalf("bitmap32 mismatch: got=%v want=%v", gotSlice, want)
	}
	if got.cardinality() != uint64(len(want)) {
		t.Fatalf("bitmap32 cardinality mismatch: got=%d want=%d", got.cardinality(), len(want))
	}
}

func assertSameLargePostingSet(t *testing.T, got *largePosting, want []uint64) {
	t.Helper()
	gotSlice := got.toArray()
	want = canonicalUint64s(want)
	assertStrictlySortedUniqueU64(t, gotSlice)
	if !slices.Equal(gotSlice, want) {
		t.Fatalf("large posting mismatch: got=%v want=%v", gotSlice, want)
	}
	if got.cardinality() != uint64(len(want)) {
		t.Fatalf("large posting cardinality mismatch: got=%d want=%d", got.cardinality(), len(want))
	}
}

func assertSameContainerSet(t *testing.T, got container16, want []uint16) {
	t.Helper()
	gotSlice := containerToSlice(got)
	want = canonicalUint16s(want)
	assertStrictlySortedUniqueU16(t, gotSlice)
	if !slices.Equal(gotSlice, want) {
		t.Fatalf("container mismatch: got=%v want=%v type=%T", gotSlice, want, got)
	}
	if got.getCardinality() != len(want) {
		t.Fatalf("container cardinality mismatch: got=%d want=%d type=%T", got.getCardinality(), len(want), got)
	}
}

func mustWriteListPayload(t *testing.T, list List) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := list.WriteTo(writer); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

func mustReadListPayload(t *testing.T, payload []byte) List {
	t.Helper()
	var out List
	if err := out.ReadFrom(bufio.NewReader(bytes.NewReader(payload))); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	return out
}

func appendTestUvarint(dst []byte, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

func encodeBitmap32Payload(containers ...[]byte) []byte {
	payload := make([]byte, bitmap32WireHeaderSize)
	binary.LittleEndian.PutUint32(payload[:bitmap32WireHeaderSize], uint32(len(containers)))
	for _, container := range containers {
		payload = append(payload, container...)
	}
	return payload
}

func encodeBitmap32ArrayWireContainer(key uint16, values ...uint16) []byte {
	payload := make([]byte, bitmap32WireContainerHeaderSize)
	binary.LittleEndian.PutUint16(payload[0:2], key)
	payload[2] = bitmap32WireContainerArray
	binary.LittleEndian.PutUint16(payload[3:5], uint16(len(values)-1))
	for _, value := range values {
		var raw [2]byte
		binary.LittleEndian.PutUint16(raw[:], value)
		payload = append(payload, raw[:]...)
	}
	return payload
}

func encodeLargePayload(keys []uint32, payloads [][]byte) []byte {
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body[:8], uint64(len(keys)))
	for i, key := range keys {
		var raw [4]byte
		binary.LittleEndian.PutUint32(raw[:], key)
		body = append(body, raw[:]...)
		body = append(body, payloads[i]...)
	}
	payload := appendTestUvarint(nil, uint64(len(body)))
	return append(payload, body...)
}

func releaseContainerPair(base, result container16) {
	if result == nil {
		releaseContainer(base)
		return
	}
	if result != base {
		releaseContainer(base)
	}
	releaseContainer(result)
}

func buildBitmap32(ids ...uint32) *bitmap32 {
	rb := newBitmap()
	rb.addMany(canonicalUint32s(ids))
	return rb
}

func buildContainerArray(ids []uint16) container16 {
	return newContainerArrayFromSlice(canonicalUint16s(ids))
}

func buildContainerBitmap(ids []uint16) container16 {
	bc := newContainerBitmap()
	ac := newContainerArrayFromSlice(canonicalUint16s(ids))
	bc.loadData(ac)
	releaseContainerArray(ac)
	return bc
}

func buildContainerRun(ids []uint16) container16 {
	ac := newContainerArrayFromSlice(canonicalUint16s(ids))
	rc := newContainerRunFromArray(ac)
	releaseContainerArray(ac)
	return rc
}

func containerFactories() []containerFactory {
	return []containerFactory{
		{name: "array", build: buildContainerArray},
		{name: "bitmap", build: buildContainerBitmap},
		{name: "run", build: buildContainerRun},
	}
}

func unionUint64(a, b []uint64) []uint64 {
	out := append(slices.Clone(a), b...)
	return canonicalUint64s(out)
}

func intersectUint64(a, b []uint64) []uint64 {
	a = canonicalUint64s(a)
	b = canonicalUint64s(b)
	out := make([]uint64, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

func differenceUint64(a, b []uint64) []uint64 {
	a = canonicalUint64s(a)
	b = canonicalUint64s(b)
	out := make([]uint64, 0, len(a))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			j++
		default:
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	return out
}

func unionUint16(a, b []uint16) []uint16 {
	out := append(slices.Clone(a), b...)
	return canonicalUint16s(out)
}

func intersectUint16(a, b []uint16) []uint16 {
	a = canonicalUint16s(a)
	b = canonicalUint16s(b)
	out := make([]uint16, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

func xorUint16(a, b []uint16) []uint16 {
	out := append(differenceUint16(a, b), differenceUint16(b, a)...)
	return canonicalUint16s(out)
}

func differenceUint16(a, b []uint16) []uint16 {
	a = canonicalUint16s(a)
	b = canonicalUint16s(b)
	out := make([]uint16, 0, len(a))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			j++
		default:
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	return out
}

func unionUint32(a, b []uint32) []uint32 {
	out := append(slices.Clone(a), b...)
	return canonicalUint32s(out)
}

func intersectUint32(a, b []uint32) []uint32 {
	a = canonicalUint32s(a)
	b = canonicalUint32s(b)
	out := make([]uint32, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

func differenceUint32(a, b []uint32) []uint32 {
	a = canonicalUint32s(a)
	b = canonicalUint32s(b)
	out := make([]uint32, 0, len(a))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			j++
		default:
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	return out
}

func xorUint32(a, b []uint32) []uint32 {
	out := append(differenceUint32(a, b), differenceUint32(b, a)...)
	return canonicalUint32s(out)
}
