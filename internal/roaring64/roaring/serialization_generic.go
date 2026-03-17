//go:build (!amd64 && !386 && !arm && !arm64 && !ppc64le && !mipsle && !mips64le && !mips64p32le && !wasm) || appengine
// +build !amd64,!386,!arm,!arm64,!ppc64le,!mipsle,!mips64le,!mips64p32le,!wasm appengine

package roaring

import (
	"encoding/binary"
	"errors"
	"io"
)

func (ac *arrayContainer) writeTo(stream io.Writer) (int, error) {
	buf := make([]byte, 2*len(ac.content))
	for i, v := range ac.content {
		base := i * 2
		buf[base] = byte(v)
		buf[base+1] = byte(v >> 8)
	}
	return stream.Write(buf)
}

func (bc *bitmapContainer) writeTo(stream io.Writer) (int, error) {
	if bc.cardinality <= arrayDefaultMaxSize {
		return 0, errors.New("refusing to write bitmap container with cardinality of array container")
	}

	buf := make([]byte, 8*len(bc.bitmap))
	for i, v := range bc.bitmap {
		base := i * 8
		buf[base] = byte(v)
		buf[base+1] = byte(v >> 8)
		buf[base+2] = byte(v >> 16)
		buf[base+3] = byte(v >> 24)
		buf[base+4] = byte(v >> 32)
		buf[base+5] = byte(v >> 40)
		buf[base+6] = byte(v >> 48)
		buf[base+7] = byte(v >> 56)
	}
	return stream.Write(buf)
}

func uint64SliceAsByteSlice(slice []uint64) []byte {
	by := make([]byte, len(slice)*8)
	for i, v := range slice {
		binary.LittleEndian.PutUint64(by[i*8:], v)
	}
	return by
}

func uint16SliceAsByteSlice(slice []uint16) []byte {
	by := make([]byte, len(slice)*2)
	for i, v := range slice {
		binary.LittleEndian.PutUint16(by[i*2:], v)
	}
	return by
}

func byteSliceAsUint16Slice(slice []byte) []uint16 {
	if len(slice)%2 != 0 {
		panic("slice size should be divisible by 2")
	}

	b := make([]uint16, len(slice)/2)
	for i := range b {
		b[i] = binary.LittleEndian.Uint16(slice[2*i:])
	}
	return b
}

func byteSliceAsUint64Slice(slice []byte) []uint64 {
	if len(slice)%8 != 0 {
		panic("slice size should be divisible by 8")
	}

	b := make([]uint64, len(slice)/8)
	for i := range b {
		b[i] = binary.LittleEndian.Uint64(slice[8*i:])
	}
	return b
}

func byteSliceAsInterval16Slice(byteSlice []byte) []interval16 {
	if len(byteSlice)%4 != 0 {
		panic("slice size should be divisible by 4")
	}

	intervalSlice := make([]interval16, len(byteSlice)/4)
	for i := range intervalSlice {
		intervalSlice[i] = interval16{
			start:  binary.LittleEndian.Uint16(byteSlice[i*4:]),
			length: binary.LittleEndian.Uint16(byteSlice[i*4+2:]),
		}
	}
	return intervalSlice
}
