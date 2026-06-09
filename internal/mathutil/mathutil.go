package mathutil

import "math/rand/v2"

const (
	maxUint64       = ^uint64(0)
	SplitMix64Gamma = 0x9e3779b97f4a7c15
	splitMix64Mul1  = 0xbf58476d1ce4e5b9
	splitMix64Mul2  = 0x94d049bb133111eb
)

func Mix64(x uint64) uint64 {
	return mix64(x + SplitMix64Gamma)
}

func NextSplitMix64(state *uint64) uint64 {
	*state += SplitMix64Gamma
	return mix64(*state)
}

func mix64(x uint64) uint64 {
	x = (x ^ (x >> 30)) * splitMix64Mul1
	x = (x ^ (x >> 27)) * splitMix64Mul2
	return x ^ (x >> 31)
}

func NewRand(seed int64) *rand.Rand {
	s := uint64(seed)
	return rand.New(rand.NewPCG(s, s^SplitMix64Gamma))
}

func SatMulUint64(a, b uint64) uint64 {
	if a == 0 || b == 0 {
		return 0
	}
	if maxUint64/a < b {
		return maxUint64
	}
	return a * b
}

func SatAddUint64(total, add uint64) uint64 {
	if maxUint64-total < add {
		return maxUint64
	}
	return total + add
}
