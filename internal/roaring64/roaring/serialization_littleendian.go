//go:build (386 && !appengine) || (amd64 && !appengine) || (arm && !appengine) || (arm64 && !appengine) || (ppc64le && !appengine) || (mipsle && !appengine) || (mips64le && !appengine) || (mips64p32le && !appengine) || (wasm && !appengine)
// +build 386,!appengine amd64,!appengine arm,!appengine arm64,!appengine ppc64le,!appengine mipsle,!appengine mips64le,!appengine mips64p32le,!appengine wasm,!appengine

package roaring

import (
	"errors"
	"io"
	"reflect"
	"runtime"
	"unsafe"
)

func (ac *arrayContainer) writeTo(stream io.Writer) (int, error) {
	buf := uint16SliceAsByteSlice(ac.content)
	return stream.Write(buf)
}

func (bc *bitmapContainer) writeTo(stream io.Writer) (int, error) {
	if bc.cardinality <= arrayDefaultMaxSize {
		return 0, errors.New("refusing to write bitmap container with cardinality of array container")
	}
	buf := uint64SliceAsByteSlice(bc.bitmap)
	return stream.Write(buf)
}

func uint64SliceAsByteSlice(slice []uint64) []byte {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&slice))
	header.Len *= 8
	header.Cap *= 8

	result := *(*[]byte)(unsafe.Pointer(&header))
	runtime.KeepAlive(&slice)
	return result
}

func uint16SliceAsByteSlice(slice []uint16) []byte {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&slice))
	header.Len *= 2
	header.Cap *= 2

	result := *(*[]byte)(unsafe.Pointer(&header))
	runtime.KeepAlive(&slice)
	return result
}

func byteSliceAsUint16Slice(slice []byte) (result []uint16) {
	if len(slice)%2 != 0 {
		panic("slice size should be divisible by 2")
	}

	bHeader := (*reflect.SliceHeader)(unsafe.Pointer(&slice))
	rHeader := (*reflect.SliceHeader)(unsafe.Pointer(&result))
	rHeader.Data = bHeader.Data
	rHeader.Len = bHeader.Len / 2
	rHeader.Cap = bHeader.Cap / 2
	runtime.KeepAlive(&slice)
	return
}

func byteSliceAsUint64Slice(slice []byte) (result []uint64) {
	if len(slice)%8 != 0 {
		panic("slice size should be divisible by 8")
	}

	bHeader := (*reflect.SliceHeader)(unsafe.Pointer(&slice))
	rHeader := (*reflect.SliceHeader)(unsafe.Pointer(&result))
	rHeader.Data = bHeader.Data
	rHeader.Len = bHeader.Len / 8
	rHeader.Cap = bHeader.Cap / 8
	runtime.KeepAlive(&slice)
	return
}

func byteSliceAsInterval16Slice(slice []byte) (result []interval16) {
	if len(slice)%4 != 0 {
		panic("slice size should be divisible by 4")
	}

	bHeader := (*reflect.SliceHeader)(unsafe.Pointer(&slice))
	rHeader := (*reflect.SliceHeader)(unsafe.Pointer(&result))
	rHeader.Data = bHeader.Data
	rHeader.Len = bHeader.Len / 4
	rHeader.Cap = bHeader.Cap / 4
	runtime.KeepAlive(&slice)
	return
}
