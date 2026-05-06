package pools

import (
	"runtime"
	"sync"
	"testing"
)

func TestGetSliceCapHints(t *testing.T) {
	testGetSliceCapHints[int](t, "int", MinNumericPooledCap, MaxNumericPooledCap, GetIntSlice)
	testGetSliceCapHints[int32](t, "int32", MinNumericPooledCap, MaxNumericPooledCap, GetInt32Slice)
	testGetSliceCapHints[int64](t, "int64", MinNumericPooledCap, MaxNumericPooledCap, GetInt64Slice)
	testGetSliceCapHints[uint](t, "uint", MinNumericPooledCap, MaxNumericPooledCap, GetUintSlice)
	testGetSliceCapHints[uint32](t, "uint32", MinNumericPooledCap, MaxNumericPooledCap, GetUint32Slice)
	testGetSliceCapHints[uint64](t, "uint64", MinNumericPooledCap, MaxNumericPooledCap, GetUint64Slice)
	testGetSliceCapHints[float32](t, "float32", MinNumericPooledCap, MaxNumericPooledCap, GetFloat32Slice)
	testGetSliceCapHints[float64](t, "float64", MinNumericPooledCap, MaxNumericPooledCap, GetFloat64Slice)
	testGetSliceCapHints[bool](t, "bool", MinNumericPooledCap, MaxNumericPooledCap, GetBoolSlice)
	testGetSliceCapHints[string](t, "string", MinStringPooledCap, MaxStringPooledCap, GetStringSlice)
}

func TestGetSliceNegativeCapHintPanics(t *testing.T) {
	assertPanic(t, "GetIntSlice", func() { GetIntSlice(-1) })
	assertPanic(t, "GetInt32Slice", func() { GetInt32Slice(-1) })
	assertPanic(t, "GetInt64Slice", func() { GetInt64Slice(-1) })
	assertPanic(t, "GetUintSlice", func() { GetUintSlice(-1) })
	assertPanic(t, "GetUint32Slice", func() { GetUint32Slice(-1) })
	assertPanic(t, "GetUint64Slice", func() { GetUint64Slice(-1) })
	assertPanic(t, "GetFloat32Slice", func() { GetFloat32Slice(-1) })
	assertPanic(t, "GetFloat64Slice", func() { GetFloat64Slice(-1) })
	assertPanic(t, "GetBoolSlice", func() { GetBoolSlice(-1) })
	assertPanic(t, "GetStringSlice", func() { GetStringSlice(-1) })
}

func TestGetSliceReusesBucketWithinProbeWindow(t *testing.T) {
	if testRaceEnabled {
		t.Skip("sync.Pool may drop values under the race detector")
	}
	forceSingleP(t)
	drainAllPools()
	t.Cleanup(drainAllPools)

	s := make([]uint64, 7, MinNumericPooledCap<<3)
	wantPtr := firstPtr(s)
	PutUint64Slice(s)

	got := GetUint64Slice(MinNumericPooledCap)
	if len(got) != 0 {
		t.Fatalf("reused slice len: got=%d want=0", len(got))
	}
	if cap(got) != MinNumericPooledCap<<3 {
		t.Fatalf("reused slice cap: got=%d want=%d", cap(got), MinNumericPooledCap<<3)
	}
	if firstPtr(got) != wantPtr {
		t.Fatalf("GetUint64Slice did not reuse bucket within probe window")
	}
}

func TestGetSliceIgnoresBucketPastProbeWindow(t *testing.T) {
	if testRaceEnabled {
		t.Skip("sync.Pool may drop values under the race detector")
	}
	forceSingleP(t)
	drainAllPools()
	t.Cleanup(drainAllPools)

	s := make([]uint64, 0, MinNumericPooledCap<<4)
	savedPtr := firstPtr(s)
	PutUint64Slice(s)

	got := GetUint64Slice(MinNumericPooledCap)
	if cap(got) != MinNumericPooledCap {
		t.Fatalf("GetUint64Slice cap: got=%d want=%d", cap(got), MinNumericPooledCap)
	}
	if firstPtr(got) == savedPtr {
		t.Fatalf("GetUint64Slice reused a bucket past the probe window")
	}
}

func TestPutSliceDemotesExternalCapacityToBucket(t *testing.T) {
	if testRaceEnabled {
		t.Skip("sync.Pool may drop values under the race detector")
	}
	forceSingleP(t)
	drainAllPools()
	t.Cleanup(drainAllPools)

	s := make([]uint64, 0, MinNumericPooledCap*3)
	wantPtr := firstPtr(s)
	PutUint64Slice(s)

	got := GetUint64Slice(MinNumericPooledCap << 1)
	if cap(got) != MinNumericPooledCap<<1 {
		t.Fatalf("demoted slice cap: got=%d want=%d", cap(got), MinNumericPooledCap<<1)
	}
	if firstPtr(got) != wantPtr {
		t.Fatalf("PutUint64Slice did not demote external capacity to the floor bucket")
	}
}

func TestPutSliceRetainsMaxRetainedCap(t *testing.T) {
	if testRaceEnabled {
		t.Skip("sync.Pool may drop values under the race detector")
	}
	forceSingleP(t)
	drainAllPools()
	t.Cleanup(drainAllPools)

	s := make([]uint64, 0, MaxNumericRetainedCap)
	wantPtr := firstPtr(s)
	PutUint64Slice(s)

	got := GetUint64Slice(MaxNumericPooledCap)
	if cap(got) != MaxNumericPooledCap {
		t.Fatalf("retained slice cap: got=%d want=%d", cap(got), MaxNumericPooledCap)
	}
	if firstPtr(got) != wantPtr {
		t.Fatalf("PutUint64Slice dropped MaxNumericRetainedCap")
	}
}

func TestPutSliceDropsAboveMaxRetainedCap(t *testing.T) {
	if testRaceEnabled {
		t.Skip("sync.Pool may drop values under the race detector")
	}
	forceSingleP(t)
	drainAllPools()
	t.Cleanup(drainAllPools)

	s := make([]uint64, 0, MaxNumericRetainedCap+1)
	droppedPtr := firstPtr(s)
	PutUint64Slice(s)

	got := GetUint64Slice(MaxNumericPooledCap)
	if cap(got) != MaxNumericPooledCap {
		t.Fatalf("fresh slice cap: got=%d want=%d", cap(got), MaxNumericPooledCap)
	}
	if firstPtr(got) == droppedPtr {
		t.Fatalf("PutUint64Slice retained capacity above MaxNumericRetainedCap")
	}
}

func TestPutStringSliceRetainsMaxRetainedCap(t *testing.T) {
	if testRaceEnabled {
		t.Skip("sync.Pool may drop values under the race detector")
	}
	forceSingleP(t)
	drainAllPools()
	t.Cleanup(drainAllPools)

	s := make([]string, 0, MaxStringRetainedCap)
	wantPtr := firstPtr(s)
	PutStringSlice(s)

	got := GetStringSlice(MaxStringPooledCap)
	if cap(got) != MaxStringPooledCap {
		t.Fatalf("retained string slice cap: got=%d want=%d", cap(got), MaxStringPooledCap)
	}
	if firstPtr(got) != wantPtr {
		t.Fatalf("PutStringSlice dropped MaxStringRetainedCap")
	}
}

func TestPutStringSliceClearsFullCapacity(t *testing.T) {
	drainAllPools()
	t.Cleanup(drainAllPools)

	s := make([]string, 1, MinStringPooledCap)
	full := s[:cap(s)]
	for i := range full {
		full[i] = "value"
	}

	PutStringSlice(s)

	for i, v := range full {
		if v != "" {
			t.Fatalf("string slot %d was not cleared", i)
		}
	}
}

func TestTypedNumericPoolsAreIndependent(t *testing.T) {
	if testRaceEnabled {
		t.Skip("sync.Pool may drop values under the race detector")
	}
	forceSingleP(t)
	drainAllPools()
	t.Cleanup(drainAllPools)

	i := make([]int, 0, MinNumericPooledCap)
	i32 := make([]int32, 0, MinNumericPooledCap)
	i64 := make([]int64, 0, MinNumericPooledCap)
	uints := make([]uint, 0, MinNumericPooledCap)
	u32 := make([]uint32, 0, MinNumericPooledCap)
	u64 := make([]uint64, 0, MinNumericPooledCap)
	f32 := make([]float32, 0, MinNumericPooledCap)
	f64 := make([]float64, 0, MinNumericPooledCap)
	iPtr := firstPtr(i)
	i32Ptr := firstPtr(i32)
	i64Ptr := firstPtr(i64)
	uintPtr := firstPtr(uints)
	u32Ptr := firstPtr(u32)
	u64Ptr := firstPtr(u64)
	f32Ptr := firstPtr(f32)
	f64Ptr := firstPtr(f64)

	PutIntSlice(i)
	PutInt32Slice(i32)
	PutInt64Slice(i64)
	PutUintSlice(uints)
	PutUint32Slice(u32)
	PutUint64Slice(u64)
	PutFloat32Slice(f32)
	PutFloat64Slice(f64)

	if got := GetIntSlice(MinNumericPooledCap); firstPtr(got) != iPtr {
		t.Fatalf("GetIntSlice used the wrong pool")
	}
	if got := GetInt32Slice(MinNumericPooledCap); firstPtr(got) != i32Ptr {
		t.Fatalf("GetInt32Slice used the wrong pool")
	}
	if got := GetInt64Slice(MinNumericPooledCap); firstPtr(got) != i64Ptr {
		t.Fatalf("GetInt64Slice used the wrong pool")
	}
	if got := GetUintSlice(MinNumericPooledCap); firstPtr(got) != uintPtr {
		t.Fatalf("GetUintSlice used the wrong pool")
	}
	if got := GetUint32Slice(MinNumericPooledCap); firstPtr(got) != u32Ptr {
		t.Fatalf("GetUint32Slice used the wrong pool")
	}
	if got := GetUint64Slice(MinNumericPooledCap); firstPtr(got) != u64Ptr {
		t.Fatalf("GetUint64Slice used the wrong pool")
	}
	if got := GetFloat32Slice(MinNumericPooledCap); firstPtr(got) != f32Ptr {
		t.Fatalf("GetFloat32Slice used the wrong pool")
	}
	if got := GetFloat64Slice(MinNumericPooledCap); firstPtr(got) != f64Ptr {
		t.Fatalf("GetFloat64Slice used the wrong pool")
	}
}

func testGetSliceCapHints[T any](t *testing.T, name string, minCap int, maxCap int, get func(int) []T) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		tests := []struct {
			name string
			hint int
			cap  int
		}{
			{name: "Zero", hint: 0, cap: minCap},
			{name: "BelowMin", hint: minCap - 1, cap: minCap},
			{name: "Min", hint: minCap, cap: minCap},
			{name: "AboveMin", hint: minCap + 1, cap: minCap << 1},
			{name: "PowerOfTwo", hint: minCap << 1, cap: minCap << 1},
			{name: "BelowNextPowerOfTwo", hint: (minCap << 1) + 1, cap: minCap << 2},
			{name: "Max", hint: maxCap, cap: maxCap},
			{name: "AboveMax", hint: maxCap + 1, cap: maxCap + 1},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				drainAllPools()

				got := get(tt.hint)
				if len(got) != 0 {
					t.Fatalf("len: got=%d want=0", len(got))
				}
				if cap(got) != tt.cap {
					t.Fatalf("cap: got=%d want=%d", cap(got), tt.cap)
				}
			})
		}
	})
}

func assertPanic(t *testing.T, name string, f func()) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic")
			}
		}()
		f()
	})
}

func firstPtr[T any](s []T) *T {
	return &s[:1][0]
}

func forceSingleP(t *testing.T) {
	t.Helper()
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(prev)
	})
}

func drainAllPools() {
	for i := range intPools {
		drainPool(&intPools[i])
		drainPool(&int32Pools[i])
		drainPool(&int64Pools[i])
		drainPool(&uintPools[i])
		drainPool(&uint32Pools[i])
		drainPool(&uint64Pools[i])
		drainPool(&float32Pools[i])
		drainPool(&float64Pools[i])
		drainPool(&boolPools[i])
	}
	for i := range stringPools {
		drainPool(&stringPools[i])
	}
}

func drainPool(pool *sync.Pool) {
	for pool.Get() != nil {
	}
}
