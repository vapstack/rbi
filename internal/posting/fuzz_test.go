package posting

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"slices"
	"testing"
)

func fuzzUint64s(data []byte) []uint64 {
	if len(data) == 0 {
		return nil
	}
	if len(data) > 8*128 {
		data = data[:8*128]
	}
	out := make([]uint64, 0, len(data)/8)
	for len(data) >= 8 {
		out = append(out, binary.LittleEndian.Uint64(data[:8]))
		data = data[8:]
	}
	return canonicalUint64s(out)
}

func fuzzUint16s(data []byte) []uint16 {
	if len(data) == 0 {
		return nil
	}
	if len(data) > 2*2048 {
		data = data[:2*2048]
	}
	out := make([]uint16, 0, len(data)/2)
	for len(data) >= 2 {
		out = append(out, binary.LittleEndian.Uint16(data[:2]))
		data = data[2:]
	}
	return canonicalUint16s(out)
}

func fuzzUint32s(data []byte) []uint32 {
	if len(data) == 0 {
		return nil
	}
	if len(data) > 4*1024 {
		data = data[:4*1024]
	}
	out := make([]uint32, 0, len(data)/4)
	for len(data) >= 4 {
		out = append(out, binary.LittleEndian.Uint32(data[:4]))
		data = data[4:]
	}
	return canonicalUint32s(out)
}

func listFromUint64Slice(ids []uint64) List {
	var out List
	for _, id := range ids {
		out.Add(id)
	}
	return out
}

func bitmap32FromUint32Slice(ids []uint32) *bitmap32 {
	rb := newBitmap32()
	rb.addMany(ids)
	return rb
}

func FuzzListRoundTrip(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		ids := fuzzUint64s(data)
		list := listFromUint64Slice(ids)
		defer list.Release()

		payload := mustWriteListPayload(t, list)
		got := mustReadListPayload(t, payload)
		defer got.Release()
		assertSameListSet(t, got, ids)

		var stream bytes.Buffer
		stream.Write(payload)
		stream.Write(payload)
		reader := bufio.NewReader(bytes.NewReader(stream.Bytes()))
		if err := Skip(reader); err != nil {
			t.Fatalf("Skip: %v", err)
		}
		var next List
		if err := next.ReadFrom(reader); err != nil {
			t.Fatalf("ReadFrom: %v", err)
		}
		defer next.Release()
		assertSameListSet(t, next, ids)
	})
}

func FuzzListSetOps(f *testing.F) {
	f.Add([]byte{}, []byte{})
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0}, []byte{1, 0, 0, 0, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, leftData, rightData []byte) {
		leftIDs := fuzzUint64s(leftData)
		rightIDs := fuzzUint64s(rightData)

		left := listFromUint64Slice(leftIDs)
		right := listFromUint64Slice(rightIDs)
		defer left.Release()
		defer right.Release()

		union := listFromUint64Slice(leftIDs)
		unionRight := listFromUint64Slice(rightIDs)
		union.OrInPlace(unionRight)
		unionRight.Release()
		defer union.Release()
		assertSameListSet(t, union, unionUint64(leftIDs, rightIDs))

		intersection := listFromUint64Slice(leftIDs)
		intersectionRight := listFromUint64Slice(rightIDs)
		intersection.AndInPlace(intersectionRight)
		intersectionRight.Release()
		defer intersection.Release()
		assertSameListSet(t, intersection, intersectUint64(leftIDs, rightIDs))

		diff := listFromUint64Slice(leftIDs)
		diffRight := listFromUint64Slice(rightIDs)
		diff.AndNotInPlace(diffRight)
		diffRight.Release()
		defer diff.Release()
		assertSameListSet(t, diff, differenceUint64(leftIDs, rightIDs))

		if got := left.Intersects(right); got != (len(intersectUint64(leftIDs, rightIDs)) > 0) {
			t.Fatalf("Intersects mismatch: got=%v", got)
		}
		if got := left.AndCardinality(right); got != uint64(len(intersectUint64(leftIDs, rightIDs))) {
			t.Fatalf("AndCardinality mismatch: got=%d", got)
		}
	})
}

func FuzzContainerSetOps(f *testing.F) {
	f.Add([]byte{}, []byte{})
	f.Add([]byte{1, 0, 2, 0, 3, 0}, []byte{2, 0, 3, 0, 4, 0})

	f.Fuzz(func(t *testing.T, leftData, rightData []byte) {
		leftIDs := fuzzUint16s(leftData)
		rightIDs := fuzzUint16s(rightData)

		for _, leftFactory := range containerFactories() {
			for _, rightFactory := range containerFactories() {
				left := leftFactory.build(leftIDs)
				right := rightFactory.build(rightIDs)

				orResult := left.or(right)
				assertSameContainerSet(t, orResult, unionUint16(leftIDs, rightIDs))
				releaseContainer(orResult)

				andResult := left.and(right)
				assertSameContainerSet(t, andResult, intersectUint16(leftIDs, rightIDs))
				releaseContainer(andResult)

				xorResult := left.xor(right)
				assertSameContainerSet(t, xorResult, xorUint16(leftIDs, rightIDs))
				releaseContainer(xorResult)

				andNotResult := left.andNot(right)
				assertSameContainerSet(t, andNotResult, differenceUint16(leftIDs, rightIDs))
				releaseContainer(andNotResult)

				releaseContainer(left)
				releaseContainer(right)
			}
		}
	})
}

func FuzzBitmap32WireRead(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0, 0})
	f.Add([]byte{1, 0, 0, 0, 2, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		ids := fuzzUint32s(data)
		src := bitmap32FromUint32Slice(ids)
		defer releaseBitmap32(src)

		var payload bytes.Buffer
		if _, err := src.WriteTo(&payload); err != nil {
			t.Fatalf("WriteTo: %v", err)
		}

		mutated := slices.Clone(payload.Bytes())
		for i := 0; i < len(mutated) && i < len(data); i++ {
			mutated[i] ^= data[i]
		}

		receiver := bitmap32FromUint32Slice([]uint32{7, 9, 11})
		defer releaseBitmap32(receiver)
		_, err := receiver.ReadFrom(bytes.NewReader(mutated))
		if err != nil {
			got := bitmap32ToSlice(receiver)
			if !slices.Equal(got, nil) && !slices.Equal(got, []uint32{7, 9, 11}) {
				t.Fatalf("bitmap32 mismatch: got=%v want old-or-empty", got)
			}
			return
		}

		got := bitmap32ToSlice(receiver)
		assertStrictlySortedUniqueU32(t, got)
	})
}
