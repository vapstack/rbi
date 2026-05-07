package posting

import (
	"bufio"
	"bytes"
	"testing"
)

var (
	postingBenchBoolSink  bool
	postingBenchUint64Sum uint64
	postingBenchSliceSink []uint64
	postingBenchErrSink   error
)

func postingBenchSmallIDs() []uint64 {
	return []uint64{3, 9, 17, 25, 33, 41, 57, 65}
}

func postingBenchMidIDs() []uint64 {
	ids := make([]uint64, MidCap)
	for i := range ids {
		ids[i] = uint64(i*3 + 1)
	}
	return ids
}

func postingBenchRunIDs(n int) []uint64 {
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	return ids
}

func postingBenchPayload(tb testing.TB, ids List) []byte {
	tb.Helper()
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := ids.WriteTo(writer); err != nil {
		tb.Fatalf("WriteTo: %v", err)
	}
	if err := writer.Flush(); err != nil {
		tb.Fatalf("Flush: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func postingBenchForEach(id uint64) bool {
	postingBenchUint64Sum += id
	return true
}

func postingBenchForEachStop(id uint64) bool {
	postingBenchUint64Sum += id
	return false
}

func BenchmarkBuildFromSorted(b *testing.B) {
	smallIDs := postingBenchSmallIDs()
	midIDs := postingBenchMidIDs()
	largeIDs := buildTestLargeIDs(8, 64, 0)
	runIDs := postingBenchRunIDs(4096)

	cases := [...]struct {
		name string
		ids  []uint64
	}{
		{"Empty", nil},
		{"Singleton", []uint64{42}},
		{"Small", smallIDs},
		{"Mid", midIDs},
		{"LargeSparse", largeIDs},
		{"LargeRun", runIDs},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := BuildFromSorted(tc.ids)
				postingBenchUint64Sum += out.Cardinality()
				out.Release()
			}
		})
	}
}

func BenchmarkListReadOnly(b *testing.B) {
	single := BuildFromSorted([]uint64{42})
	small := BuildFromSorted(postingBenchSmallIDs())
	midIDs := postingBenchMidIDs()
	mid := BuildFromSorted(midIDs)
	largeIDs := buildTestLargeIDs(8, 64, 0)
	large := BuildFromSorted(largeIDs)
	defer single.Release()
	defer small.Release()
	defer mid.Release()
	defer large.Release()

	cases := [...]struct {
		name string
		ids  List
		hit  uint64
		miss uint64
	}{
		{"Empty", List{}, 1, 2},
		{"Singleton", single, 42, 43},
		{"Small", small, 25, 26},
		{"Mid", mid, midIDs[len(midIDs)/2], midIDs[len(midIDs)/2] + 1},
		{"Large", large, largeIDs[len(largeIDs)/2], largeIDs[len(largeIDs)/2] + 1},
	}

	for _, tc := range cases {
		b.Run(tc.name+"/IsEmpty", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = tc.ids.IsEmpty()
			}
		})
		b.Run(tc.name+"/Cardinality", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchUint64Sum += tc.ids.Cardinality()
			}
		})
		b.Run(tc.name+"/ContainsHit", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = tc.ids.Contains(tc.hit)
			}
		})
		b.Run(tc.name+"/ContainsMiss", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = tc.ids.Contains(tc.miss)
			}
		})
		b.Run(tc.name+"/TrySingle", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				id, ok := tc.ids.TrySingle()
				postingBenchUint64Sum += id
				postingBenchBoolSink = ok
			}
		})
		b.Run(tc.name+"/Minimum", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				id, ok := tc.ids.Minimum()
				postingBenchUint64Sum += id
				postingBenchBoolSink = ok
			}
		})
		b.Run(tc.name+"/Maximum", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				id, ok := tc.ids.Maximum()
				postingBenchUint64Sum += id
				postingBenchBoolSink = ok
			}
		})
		b.Run(tc.name+"/SizeInBytes", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchUint64Sum += tc.ids.SizeInBytes()
			}
		})
	}
}

func BenchmarkListIteration(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs())
	mid := BuildFromSorted(postingBenchMidIDs())
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	defer small.Release()
	defer mid.Release()
	defer large.Release()

	cases := [...]struct {
		name string
		ids  List
	}{
		{"Empty", List{}},
		{"Small", small},
		{"Mid", mid},
		{"Large", large},
	}
	for _, tc := range cases {
		b.Run(tc.name+"/Iter", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				it := tc.ids.Iter()
				var sum uint64
				for it.HasNext() {
					sum += it.Next()
				}
				it.Release()
				postingBenchUint64Sum += sum
			}
		})
		b.Run(tc.name+"/ForEach", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = tc.ids.ForEach(postingBenchForEach)
			}
		})
		b.Run(tc.name+"/ForEachStop", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = tc.ids.ForEach(postingBenchForEachStop)
			}
		})
	}
}

func BenchmarkListToArray(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs())
	mid := BuildFromSorted(postingBenchMidIDs())
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	defer small.Release()
	defer mid.Release()
	defer large.Release()

	cases := [...]struct {
		name string
		ids  List
	}{
		{"Empty", List{}},
		{"Small", small},
		{"Mid", mid},
		{"Large", large},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchSliceSink = tc.ids.ToArray()
			}
		})
	}
}

func BenchmarkListTryAppendCompactTo(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs())
	mid := BuildFromSorted(postingBenchMidIDs())
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	defer small.Release()
	defer mid.Release()
	defer large.Release()

	cases := [...]struct {
		name string
		ids  List
	}{
		{"Small", small},
		{"Mid", mid},
		{"Large", large},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			buf := make([]uint64, 0, MidCap)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, ok := tc.ids.TryAppendCompactTo(buf[:0])
				postingBenchUint64Sum += uint64(len(out))
				postingBenchBoolSink = ok
			}
		})
	}
}

func BenchmarkListClone(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs())
	mid := BuildFromSorted(postingBenchMidIDs())
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	defer small.Release()
	defer mid.Release()
	defer large.Release()

	cases := [...]struct {
		name string
		ids  List
	}{
		{"Small", small},
		{"Mid", mid},
		{"Large", large},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := tc.ids.Clone()
				postingBenchUint64Sum += out.Cardinality()
				out.Release()
			}
		})
	}
}

func BenchmarkListCloneInto(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs())
	mid := BuildFromSorted(postingBenchMidIDs())
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	otherLarge := BuildFromSorted(buildTestLargeIDs(8, 64, 32))
	defer small.Release()
	defer mid.Release()
	defer large.Release()
	defer otherLarge.Release()

	cases := [...]struct {
		name string
		src  List
		dst  List
	}{
		{"SmallIntoMid", small, mid},
		{"MidIntoSmall", mid, small},
		{"LargeIntoLarge", large, otherLarge},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				dst := tc.dst.Clone()
				out := tc.src.CloneInto(dst)
				postingBenchUint64Sum += out.Cardinality()
				out.Release()
			}
		})
	}
}

func BenchmarkListBuildAdded(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs()[:SmallCap-1])
	mid := BuildFromSorted(postingBenchMidIDs()[:MidCap-1])
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	defer small.Release()
	defer mid.Release()
	defer large.Release()

	b.Run("EmptyToSingleton", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := (List{}).BuildAdded(42)
			postingBenchUint64Sum += out.Cardinality()
			out.Release()
		}
	})

	cases := [...]struct {
		name string
		ids  List
		add  uint64
	}{
		{"SmallInsert", small, 34},
		{"MidInsert", mid, 95},
		{"LargeInsert", large, 15<<32 | 7},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := tc.ids.Clone()
				out = out.BuildAdded(tc.add)
				postingBenchUint64Sum += out.Cardinality()
				out.Release()
			}
		})
	}
}

func BenchmarkListBuildAddedChecked(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs()[:SmallCap-1])
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	defer small.Release()
	defer large.Release()

	cases := [...]struct {
		name string
		ids  List
		add  uint64
	}{
		{"SmallNew", small, 34},
		{"SmallExisting", small, 33},
		{"LargeNew", large, 15<<32 | 7},
		{"LargeExisting", large, 1<<32 | 65},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := tc.ids.Clone()
				var added bool
				out, added = out.BuildAddedChecked(tc.add)
				postingBenchBoolSink = added
				postingBenchUint64Sum += out.Cardinality()
				out.Release()
			}
		})
	}
}

func BenchmarkListBuildAddedMany(b *testing.B) {
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	addSorted := buildTestLargeIDs(4, 32, 16)
	addUnsorted := []uint64{
		21<<32 | 19,
		18<<32 | 7,
		20<<32 | 13,
		18<<32 | 3,
		21<<32 | 5,
		20<<32 | 1,
	}
	defer large.Release()

	cases := [...]struct {
		name string
		base List
		add  []uint64
	}{
		{"EmptySmall", List{}, postingBenchSmallIDs()},
		{"EmptyLargeSorted", List{}, addSorted},
		{"LargeSorted", large, addSorted},
		{"LargeUnsorted", large, addUnsorted},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := tc.base.Clone()
				out = out.BuildAddedMany(tc.add)
				postingBenchUint64Sum += out.Cardinality()
				out.Release()
			}
		})
	}
}

func BenchmarkListBuildRemoved(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs())
	midIDs := postingBenchMidIDs()
	mid := BuildFromSorted(midIDs)
	largeIDs := buildTestLargeIDs(8, 64, 0)
	large := BuildFromSorted(largeIDs)
	defer small.Release()
	defer mid.Release()
	defer large.Release()

	cases := [...]struct {
		name   string
		ids    List
		remove uint64
	}{
		{"SmallHit", small, 25},
		{"SmallMiss", small, 26},
		{"MidHit", mid, midIDs[len(midIDs)/2]},
		{"LargeHit", large, largeIDs[len(largeIDs)/2]},
		{"LargeMiss", large, 20<<32 | 1},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := tc.ids.Clone()
				out = out.BuildRemoved(tc.remove)
				postingBenchUint64Sum += out.Cardinality()
				out.Release()
			}
		})
	}
}

func BenchmarkListSetOps(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs())
	mid := BuildFromSorted(postingBenchMidIDs())
	largeLeftIDs := buildTestLargeIDs(8, 64, 0)
	largeRightIDs := buildTestLargeIDs(8, 64, 0)
	for high := 0; high < 8; high++ {
		base := uint64(high) << 32
		for i := 0; i < 64; i++ {
			largeRightIDs[high*64+i] = base + uint64(i*2+65)
		}
	}
	largeLeft := BuildFromSorted(largeLeftIDs)
	largeRight := BuildFromSorted(largeRightIDs)
	compactMask := BuildFromSorted([]uint64{
		largeLeftIDs[3], largeLeftIDs[19], largeLeftIDs[61], largeLeftIDs[127],
		largeLeftIDs[251], largeLeftIDs[383], largeLeftIDs[449], largeLeftIDs[511],
	})
	defer small.Release()
	defer mid.Release()
	defer largeLeft.Release()
	defer largeRight.Release()
	defer compactMask.Release()

	b.Run("BuildAnd/Compact", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := mid.Clone()
			out = out.BuildAnd(small)
			postingBenchUint64Sum += out.Cardinality()
			out.Release()
		}
	})
	b.Run("BuildAnd/Large", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := largeLeft.Clone()
			out = out.BuildAnd(largeRight)
			postingBenchUint64Sum += out.Cardinality()
			out.Release()
		}
	})
	b.Run("BuildAnd/LargeCompactMask", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := largeLeft.Clone()
			out = out.BuildAnd(compactMask)
			postingBenchUint64Sum += out.Cardinality()
			out.Release()
		}
	})
	b.Run("BuildOr/Compact", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := small.Clone()
			out = out.BuildOr(mid)
			postingBenchUint64Sum += out.Cardinality()
			out.Release()
		}
	})
	b.Run("BuildOr/Large", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := largeLeft.Clone()
			out = out.BuildOr(largeRight)
			postingBenchUint64Sum += out.Cardinality()
			out.Release()
		}
	})
	b.Run("BuildAndNot/Compact", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := mid.Clone()
			out = out.BuildAndNot(small)
			postingBenchUint64Sum += out.Cardinality()
			out.Release()
		}
	})
	b.Run("BuildAndNot/Large", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := largeLeft.Clone()
			out = out.BuildAndNot(largeRight)
			postingBenchUint64Sum += out.Cardinality()
			out.Release()
		}
	})
	b.Run("BuildMergedOwned/Large", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := largeLeft.Clone()
			add := largeRight.Clone()
			out = out.BuildMergedOwned(add)
			postingBenchUint64Sum += out.Cardinality()
			out.Release()
		}
	})
}

func BenchmarkListPredicates(b *testing.B) {
	largeLeftIDs := buildTestLargeIDs(8, 64, 0)
	largeRightIDs := buildTestLargeIDs(8, 64, 0)
	for high := 0; high < 8; high++ {
		base := uint64(high) << 32
		for i := 0; i < 64; i++ {
			largeRightIDs[high*64+i] = base + uint64(i*2+65)
		}
	}
	largeLeft := BuildFromSorted(largeLeftIDs)
	largeRight := BuildFromSorted(largeRightIDs)
	largeDisjoint := BuildFromSorted(buildTestLargeIDs(8, 64, 64))
	defer largeLeft.Release()
	defer largeRight.Release()
	defer largeDisjoint.Release()

	b.Run("AndCardinality", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchUint64Sum += largeLeft.AndCardinality(largeRight)
		}
	})
	b.Run("IntersectsHit", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchBoolSink = largeLeft.Intersects(largeRight)
		}
	})
	b.Run("IntersectsMiss", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchBoolSink = largeLeft.Intersects(largeDisjoint)
		}
	})
	b.Run("ForEachIntersecting", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchBoolSink = largeLeft.ForEachIntersecting(largeRight, postingBenchForEach)
		}
	})
	b.Run("ForEachIntersectingStop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchBoolSink = largeLeft.ForEachIntersecting(largeRight, postingBenchForEachStop)
		}
	})
}

func BenchmarkListBuildOptimized(b *testing.B) {
	var base List
	base = base.BuildAddedMany(postingBenchRunIDs(4096))
	defer base.Release()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := base.Clone()
		out = out.BuildOptimized()
		postingBenchUint64Sum += out.Cardinality()
		out.Release()
	}
}

func BenchmarkListTryBuildAndAnyBuf(b *testing.B) {
	midIDs := postingBenchMidIDs()
	mid := BuildFromSorted(midIDs)
	postA := BuildFromSorted([]uint64{midIDs[1], midIDs[5], midIDs[9], midIDs[13], midIDs[17], midIDs[21]})
	postB := BuildFromSorted([]uint64{midIDs[5], midIDs[7], midIDs[11], midIDs[17], midIDs[23], midIDs[27]})
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	posts := []List{postA, postB}
	defer mid.Release()
	defer postA.Release()
	defer postB.Release()
	defer large.Release()

	b.Run("Compact", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := mid.Clone()
			var ok bool
			out, ok = out.TryBuildAndAnyBuf(posts)
			postingBenchBoolSink = ok
			postingBenchUint64Sum += out.Cardinality()
			out.Release()
		}
	})
	b.Run("LargeFallback", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := large.Clone()
			var ok bool
			out, ok = out.TryBuildAndAnyBuf(posts)
			postingBenchBoolSink = ok
			out.Release()
		}
	})
}

func BenchmarkListTryResetOwnedCompactLikeFromSorted(b *testing.B) {
	midIDs := postingBenchMidIDs()
	src := BuildFromSorted(midIDs)
	rewrite := []uint64{midIDs[1], midIDs[5], midIDs[9], midIDs[13], midIDs[17], midIDs[21]}
	defer src.Release()

	var scratch List
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prev := scratch
		out, next, ok := scratch.TryResetOwnedCompactLikeFromSorted(src, rewrite)
		if !next.SharesPayload(prev) {
			prev.Release()
		}
		scratch = next
		postingBenchBoolSink = ok
		postingBenchUint64Sum += out.Cardinality()
	}
	b.StopTimer()
	scratch.Release()
}

func BenchmarkListTryResetOwnedLargeFromSorted(b *testing.B) {
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	rewrite := buildTestLargeIDs(4, 64, 32)
	defer large.Release()

	scratch := large.Clone()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ok bool
		scratch, ok = scratch.TryResetOwnedLargeFromSorted(rewrite)
		postingBenchBoolSink = ok
		postingBenchUint64Sum += scratch.Cardinality()
	}
	b.StopTimer()
	scratch.Release()
}

func BenchmarkListOwnership(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs())
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	borrowedSmall := small.Borrow()
	borrowedLarge := large.Borrow()
	defer small.Release()
	defer large.Release()

	b.Run("BorrowSmall", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := small.Borrow()
			postingBenchBoolSink = out.IsBorrowed()
			out.Release()
		}
	})
	b.Run("BorrowLarge", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := large.Borrow()
			postingBenchBoolSink = out.IsBorrowed()
			out.Release()
		}
	})
	b.Run("IsBorrowedSmall", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchBoolSink = borrowedSmall.IsBorrowed()
		}
	})
	b.Run("IsBorrowedLarge", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchBoolSink = borrowedLarge.IsBorrowed()
		}
	})
	b.Run("IsOwnedLarge", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchBoolSink = large.IsOwnedLarge()
		}
	})
	b.Run("SharesPayload", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchBoolSink = large.SharesPayload(borrowedLarge)
		}
	})
	b.Run("Release", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := large.Clone()
			out.Release()
		}
	})
}

func BenchmarkPostingIO(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs())
	mid := BuildFromSorted(postingBenchMidIDs())
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	defer small.Release()
	defer mid.Release()
	defer large.Release()

	cases := [...]struct {
		name string
		ids  List
	}{
		{"Empty", List{}},
		{"Small", small},
		{"Mid", mid},
		{"Large", large},
	}
	for _, tc := range cases {
		payload := postingBenchPayload(b, tc.ids)
		b.Run(tc.name+"/WriteTo", func(b *testing.B) {
			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf.Reset()
				writer.Reset(&buf)
				err := tc.ids.WriteTo(writer)
				if err == nil {
					err = writer.Flush()
				}
				postingBenchErrSink = err
				postingBenchUint64Sum += uint64(buf.Len())
			}
		})
		b.Run(tc.name+"/ReadFrom", func(b *testing.B) {
			var raw bytes.Reader
			reader := bufio.NewReader(&raw)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				raw.Reset(payload)
				reader.Reset(&raw)
				out, err := ReadFrom(reader)
				postingBenchErrSink = err
				postingBenchUint64Sum += out.Cardinality()
				out.Release()
			}
		})
		b.Run(tc.name+"/Skip", func(b *testing.B) {
			var raw bytes.Reader
			reader := bufio.NewReader(&raw)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				raw.Reset(payload)
				reader.Reset(&raw)
				postingBenchErrSink = Skip(reader)
			}
		})
	}
}

func BenchmarkPostingReleaseAll(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs())
	mid := BuildFromSorted(postingBenchMidIDs())
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	defer small.Release()
	defer mid.Release()
	defer large.Release()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		posts := [3]List{small.Clone(), mid.Clone(), large.Clone()}
		ReleaseAll(posts[:])
	}
}

func BenchmarkPostingReleaseMap(b *testing.B) {
	small := BuildFromSorted(postingBenchSmallIDs())
	mid := BuildFromSorted(postingBenchMidIDs())
	large := BuildFromSorted(buildTestLargeIDs(8, 64, 0))
	posts := make(map[string]List, 3)
	defer small.Release()
	defer mid.Release()
	defer large.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		posts["small"] = small.Clone()
		posts["mid"] = mid.Clone()
		posts["large"] = large.Clone()
		ReleaseMap(posts)
	}
}
