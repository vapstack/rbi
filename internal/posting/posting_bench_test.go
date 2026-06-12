package posting

import (
	"bufio"
	"bytes"
	"strconv"
	"testing"

	"github.com/vapstack/rbi/internal/mathutil"
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
	smallIDs := postingBenchSmallIDs()
	midIDs := postingBenchMidIDs()
	largeIDs := buildTestLargeIDs(8, 64, 0)
	small := BuildFromSorted(smallIDs)
	mid := BuildFromSorted(midIDs)
	large := BuildFromSorted(largeIDs)
	defer small.Release()
	defer mid.Release()
	defer large.Release()

	cases := [...]struct {
		name       string
		ids        List
		advanceMax uint64
	}{
		{"Empty", List{}, 0},
		{"Small", small, smallIDs[len(smallIDs)/2]},
		{"Mid", mid, midIDs[len(midIDs)/2]},
		{"Large", large, largeIDs[len(largeIDs)/2]},
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
		b.Run(tc.name+"/DescIter", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				it := tc.ids.DescIter()
				var sum uint64
				for it.HasNext() {
					sum += it.Next()
				}
				it.Release()
				postingBenchUint64Sum += sum
			}
		})
		b.Run(tc.name+"/DescAdvancingIter", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				it := tc.ids.DescAdvancingIter()
				it.AdvanceIfNeeded(tc.advanceMax)
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

func BenchmarkListDescAdvance(b *testing.B) {
	arrayPayload := BuildFromSorted(buildTestLargeIDs(128, 64, 0))
	bitmapPayload := BuildFromSorted(buildTestLargeIDs(32, 4097, 0))
	runBitmap := getBitmap32()
	intervals := make([]interval16, 1024)
	for i := range intervals {
		start := uint16(i * 4)
		intervals[i] = newInterval16Range(start, start+1)
	}
	runBitmap.highlowcontainer.appendContainer(0, newContainerRunCopyIv(intervals))
	runPosting := getLargePosting()
	runPosting.highlowcontainer.appendContainer(0, runBitmap)
	runPayload := largeValue(runPosting)
	defer arrayPayload.Release()
	defer bitmapPayload.Release()
	defer runPayload.Release()

	cases := [...]struct {
		name string
		ids  List
		max  uint64
	}{
		{"LargeArrayPayloadsSkipFar", arrayPayload, 2<<32 | 65},
		{"LargeBitmapPayloadsSkipFar", bitmapPayload, 2<<32 | 4097},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				it := tc.ids.DescAdvancingIter()
				it.AdvanceIfNeeded(tc.max)
				if it.HasNext() {
					postingBenchUint64Sum += it.Next()
				}
				it.Release()
			}
		})
	}

	b.Run("LargeRunPayloadRepeatedAdvance", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it := runPayload.DescAdvancingIter()
			var sum uint64
			for max := 4000; max > 0; max -= 251 {
				it.AdvanceIfNeeded(uint64(max))
				if it.HasNext() {
					sum += it.Next()
				}
			}
			it.Release()
			postingBenchUint64Sum += sum
		}
	})
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
	addMid := postingBenchMidIDs()
	addSorted := buildTestLargeIDs(4, 32, 16)
	addUnsortedSmall := []uint64{17, 3, 9, 25, 9, 1, 33, 5}
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
		{"EmptySmallUnsorted", List{}, addUnsortedSmall},
		{"EmptyMid", List{}, addMid},
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
		ReleaseMapString(posts)
	}
}

var (
	postingBenchIntSink    int
	postingBenchUint16Sink uint16
)

func postingBenchBitmapWords(shape string, seed uint64) []uint64 {
	words := make([]uint64, bitmapContainerWords)
	switch shape {
	case "empty":
	case "full":
		for i := range words {
			words[i] = ^uint64(0)
		}
	case "sparse":
		for i := range words {
			words[i] = uint64(1) << uint((uint64(i)+seed)&63)
		}
	case "dense":
		x := mathutil.Mix64(seed)
		for i := range words {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			words[i] = x
		}
	case "half0":
		for i := range words {
			if i&1 == 0 {
				words[i] = ^uint64(0)
			}
		}
	case "half1":
		for i := range words {
			if i&3 < 2 {
				words[i] = ^uint64(0)
			}
		}
	}
	return words
}

func postingBenchBitmapContainerFromWords(words []uint64) *containerBitmap {
	bc := newContainerBitmap()
	copy(bc.bitmap, words)
	bc.cardinality = int(popcntSlice(bc.bitmap))
	return bc
}

func postingBenchUint16Dense(n, start, step int) []uint16 {
	out := make([]uint16, n)
	for i := range out {
		out[i] = uint16(start + i*step)
	}
	return out
}

func postingBenchContainerBitmapFromValues(values []uint16) *containerBitmap {
	bc := newContainerBitmap()
	for i := range values {
		v := values[i]
		bc.bitmap[v>>6] |= uint64(1) << (v & 63)
	}
	bc.cardinality = len(values)
	return bc
}

func postingBenchSparseRunContainer(n int) *containerRun {
	ac := getContainerArrayFromSlice(postingBenchUint16Dense(n, 0, 4))
	rc := newContainerRunFromArray(ac)
	ac.release()
	return rc
}

func postingBenchBitmap32FromIDs(ids []uint32, optimize bool) *bitmap32 {
	rb := getBitmap32()
	rb.addManySorted(ids)
	if optimize {
		rb.runOptimize()
	}
	return rb
}

func postingBenchBitmap32RangeIDs(highCount, perHigh int) []uint32 {
	ids := make([]uint32, 0, highCount*perHigh)
	for high := 0; high < highCount; high++ {
		base := uint32(high) << 16
		for i := 0; i < perHigh; i++ {
			ids = append(ids, base+uint32(i))
		}
	}
	return ids
}

func postingBenchLargeListFromSorted(ids []uint64) List {
	lp := getLargePosting()
	lp.loadSortedUnique(ids)
	return largeValue(lp)
}

func postingBenchInterleavedLargeIDs(highCount, perHigh int, odd bool) []uint64 {
	ids := make([]uint64, 0, highCount*perHigh)
	start := 0
	if odd {
		start = 1
	}
	for high := start; high < highCount*2; high += 2 {
		base := uint64(high) << 32
		for i := 0; i < perHigh; i++ {
			ids = append(ids, base+uint64(i*2+1))
		}
	}
	return ids
}

func BenchmarkBitmapWordKernels(b *testing.B) {
	cases := [...]struct {
		name  string
		left  []uint64
		right []uint64
	}{
		{"Empty", postingBenchBitmapWords("empty", 0), postingBenchBitmapWords("empty", 1)},
		{"Full", postingBenchBitmapWords("full", 0), postingBenchBitmapWords("full", 1)},
		{"Sparse", postingBenchBitmapWords("sparse", 0), postingBenchBitmapWords("sparse", 17)},
		{"Dense", postingBenchBitmapWords("dense", 0), postingBenchBitmapWords("dense", 1)},
		{"HalfOverlap", postingBenchBitmapWords("half0", 0), postingBenchBitmapWords("half1", 0)},
	}
	ops := [...]struct {
		name string
		fn   func([]uint64, []uint64, []uint64) int
	}{
		{"Or", bitmapOrSlice},
		{"And", bitmapAndSlice},
		{"Xor", bitmapXorSlice},
		{"AndNot", bitmapAndNotSlice},
	}
	for _, op := range ops {
		for _, tc := range cases {
			b.Run(op.name+"/"+tc.name+"/Fresh", func(b *testing.B) {
				dst := make([]uint64, bitmapContainerWords)
				b.SetBytes(int64(len(dst) * 8))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					postingBenchIntSink += op.fn(dst, tc.left, tc.right)
				}
			})
			b.Run(op.name+"/"+tc.name+"/InPlace", func(b *testing.B) {
				dst := make([]uint64, bitmapContainerWords)
				copy(dst, tc.left)
				b.SetBytes(int64(len(dst) * 8))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					postingBenchIntSink += op.fn(dst, dst, tc.right)
				}
			})
		}
	}
}

func BenchmarkBitmapPredicates(b *testing.B) {
	left := postingBenchBitmapWords("dense", 3)
	right := postingBenchBitmapWords("dense", 4)
	b.Run("PopcntSlice", func(b *testing.B) {
		b.SetBytes(int64(len(left) * 8))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchUint64Sum += popcntSlice(left)
		}
	})
	b.Run("PopcntAndSlice", func(b *testing.B) {
		b.SetBytes(int64(len(left) * 8))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchUint64Sum += popcntAndSlice(left, right)
		}
	})
	b.Run("PopcntMaskSlice", func(b *testing.B) {
		b.SetBytes(int64(len(left) * 8))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchUint64Sum += popcntMaskSlice(left, right)
		}
	})
	b.Run("PopcntXorSlice", func(b *testing.B) {
		b.SetBytes(int64(len(left) * 8))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postingBenchUint64Sum += popcntXorSlice(left, right)
		}
	})

	equalRight := append([]uint64(nil), left...)
	mismatchFirst := append([]uint64(nil), left...)
	mismatchFirst[0] ^= 1
	mismatchLast := append([]uint64(nil), left...)
	mismatchLast[len(mismatchLast)-1] ^= 1
	equalsCases := [...]struct {
		name  string
		other []uint64
	}{
		{"Equal", equalRight},
		{"MismatchFirst", mismatchFirst},
		{"MismatchLast", mismatchLast},
	}
	for _, tc := range equalsCases {
		b.Run("BitmapEquals/"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(left) * 8))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = bitmapEquals(left, tc.other)
			}
		})
	}

	hitFirstLeft := postingBenchBitmapWords("empty", 0)
	hitFirstRight := postingBenchBitmapWords("empty", 0)
	hitFirstLeft[0] = 1
	hitFirstRight[0] = 1
	hitLastLeft := postingBenchBitmapWords("empty", 0)
	hitLastRight := postingBenchBitmapWords("empty", 0)
	hitLastLeft[len(hitLastLeft)-1] = 1 << 63
	hitLastRight[len(hitLastRight)-1] = 1 << 63
	missLeft := postingBenchBitmapWords("half0", 0)
	missRight := postingBenchBitmapWords("empty", 0)
	for i := 1; i < len(missRight); i += 2 {
		missRight[i] = ^uint64(0)
	}
	intersectsCases := [...]struct {
		name  string
		left  []uint64
		right []uint64
	}{
		{"HitFirst", hitFirstLeft, hitFirstRight},
		{"HitLast", hitLastLeft, hitLastRight},
		{"Miss", missLeft, missRight},
	}
	for _, tc := range intersectsCases {
		b.Run("IntersectsBitmap/"+tc.name, func(b *testing.B) {
			l := postingBenchBitmapContainerFromWords(tc.left)
			r := postingBenchBitmapContainerFromWords(tc.right)
			b.Cleanup(func() {
				l.release()
				r.release()
			})
			b.SetBytes(int64(len(tc.left) * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = l.intersectsBitmap(r)
			}
		})
	}
}

func BenchmarkBitmapRangeOps(b *testing.B) {
	ranges := [...]struct {
		name       string
		start, end int
	}{
		{"SingleWord", 128, 192},
		{"FewWords", 128, 512},
		{"HalfBitmap", 0, maxCapacity / 2},
		{"FullBitmap", 0, maxCapacity},
	}
	ops := [...]struct {
		name string
		fn   func([]uint64, int, int) int
	}{
		{"Set", setBitmapRangeAndCardinalityChange},
		{"Reset", resetBitmapRangeAndCardinalityChange},
		{"Flip", flipBitmapRangeAndCardinalityChange},
	}
	for _, op := range ops {
		for _, tc := range ranges {
			b.Run(op.name+"/"+tc.name, func(b *testing.B) {
				words := postingBenchBitmapWords("dense", 5)
				wordBytes := ((tc.end - tc.start + 63) / 64) * 8
				b.SetBytes(int64(wordBytes))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					postingBenchIntSink += op.fn(words, tc.start, tc.end)
				}
			})
		}
	}
}

func BenchmarkBitmapExpansion(b *testing.B) {
	cases := [...]struct {
		name  string
		words []uint64
	}{
		{"Sparse", postingBenchBitmapWords("sparse", 6)},
		{"Medium", postingBenchBitmapWords("dense", 7)},
		{"Full", postingBenchBitmapWords("full", 0)},
	}
	for _, tc := range cases {
		b.Run("FillArray/"+tc.name, func(b *testing.B) {
			bc := postingBenchBitmapContainerFromWords(tc.words)
			dst := make([]uint16, bc.cardinality)
			b.Cleanup(bc.release)
			b.SetBytes(int64(len(tc.words) * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bc.fillArray(dst)
				postingBenchUint16Sink = dst[len(dst)-1]
			}
		})
		b.Run("AppendBitmapWordValues/"+tc.name, func(b *testing.B) {
			dst := make([]uint16, maxCapacity)
			b.SetBytes(int64(len(tc.words) * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pos := 0
				for word := range tc.words {
					pos = appendBitmapWordValues(dst, pos, tc.words[word], word*64)
				}
				postingBenchIntSink += pos
			}
		})
		b.Run("FillArrayBitmapAndRun/"+tc.name, func(b *testing.B) {
			bc := postingBenchBitmapContainerFromWords(tc.words)
			rc := newContainerRunRange(0, MaxUint16)
			dst := make([]uint16, bc.cardinality)
			b.Cleanup(func() {
				bc.release()
				rc.release()
			})
			b.SetBytes(int64(len(tc.words) * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				postingBenchIntSink += fillArrayBitmapAndRun(dst, bc.bitmap, rc.iv)
			}
		})
	}
}

func postingBenchSortedUint16Pair(size int, shape string) ([]uint16, []uint16) {
	left := make([]uint16, size)
	right := make([]uint16, size)
	switch shape {
	case "EqualDenseOverlap":
		for i := 0; i < size; i++ {
			left[i] = uint16(i)
			right[i] = uint16(i)
		}
	case "Disjoint":
		for i := 0; i < size; i++ {
			left[i] = uint16(i)
			right[i] = uint16(32768 + i)
		}
	case "Alternating":
		for i := 0; i < size; i++ {
			left[i] = uint16(i * 2)
			right[i] = uint16(i*2 + 1)
		}
	case "Skew1_64":
		small := (size + 63) / 64
		left = make([]uint16, small)
		for i := 0; i < small; i++ {
			left[i] = uint16(i * 64)
		}
		for i := 0; i < size; i++ {
			right[i] = uint16(i)
		}
	case "EarlyIntersection":
		right[0] = 0
		for i := 0; i < size; i++ {
			left[i] = uint16(i)
			if i > 0 {
				right[i] = uint16(32768 + i)
			}
		}
	case "LateIntersection":
		for i := 0; i < size; i++ {
			left[i] = uint16(i * 2)
			right[i] = uint16(i*2 + 1)
		}
		right[size-1] = left[size-1]
	}
	return left, right
}

func BenchmarkSortedUint16SetKernels(b *testing.B) {
	sizes := [...]int{8, 32, 128, 1024, 4096}
	shapes := [...]string{"EqualDenseOverlap", "Disjoint", "Alternating", "Skew1_64", "EarlyIntersection", "LateIntersection"}
	for _, size := range sizes {
		for _, shape := range shapes {
			left, right := postingBenchSortedUint16Pair(size, shape)
			name := strconv.Itoa(size) + "/" + shape
			buffer := make([]uint16, len(left)+len(right))
			b.Run("Union/"+name, func(b *testing.B) {
				b.SetBytes(int64((len(left) + len(right)) * 2))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					postingBenchIntSink += union2by2(left, right, buffer)
				}
			})
			b.Run("Difference/"+name, func(b *testing.B) {
				b.SetBytes(int64((len(left) + len(right)) * 2))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					postingBenchIntSink += difference(left, right, buffer)
				}
			})
			b.Run("ExclusiveUnion/"+name, func(b *testing.B) {
				b.SetBytes(int64((len(left) + len(right)) * 2))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					postingBenchIntSink += exclusiveUnion2by2(left, right, buffer)
				}
			})
			b.Run("Intersection/"+name, func(b *testing.B) {
				b.SetBytes(int64((len(left) + len(right)) * 2))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					postingBenchIntSink += intersection2by2(left, right, buffer)
				}
			})
			b.Run("IntersectionCardinality/"+name, func(b *testing.B) {
				b.SetBytes(int64((len(left) + len(right)) * 2))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					postingBenchIntSink += intersection2by2Cardinality(left, right)
				}
			})
			b.Run("Intersects/"+name, func(b *testing.B) {
				b.SetBytes(int64((len(left) + len(right)) * 2))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					postingBenchBoolSink = intersects2by2(left, right)
				}
			})
		}
	}
}

func BenchmarkContainerSetOpsMatrix(b *testing.B) {
	type pair struct {
		name        string
		left, right container16
	}
	pairs := [...]pair{
		{"ArrayArray8", getContainerArrayFromSlice(postingBenchUint16Dense(8, 0, 2)), getContainerArrayFromSlice(postingBenchUint16Dense(8, 4, 2))},
		{"ArrayArray4096", getContainerArrayFromSlice(postingBenchUint16Dense(4096, 0, 2)), getContainerArrayFromSlice(postingBenchUint16Dense(4096, 1, 2))},
		{"ArrayBitmap32_4097", getContainerArrayFromSlice(postingBenchUint16Dense(32, 0, 4)), postingBenchContainerBitmapFromValues(postingBenchUint16Dense(4097, 0, 2))},
		{"ArrayRun1024", getContainerArrayFromSlice(postingBenchUint16Dense(1024, 0, 2)), newContainerRunRange(512, 4095)},
		{"BitmapBitmap4097", postingBenchContainerBitmapFromValues(postingBenchUint16Dense(4097, 0, 2)), postingBenchContainerBitmapFromValues(postingBenchUint16Dense(4097, 4096, 2))},
		{"BitmapRun4097", postingBenchContainerBitmapFromValues(postingBenchUint16Dense(4097, 0, 2)), newContainerRunRange(2048, 12287)},
		{"BitmapRunDense8192", postingBenchContainerBitmapFromValues(postingBenchUint16Dense(8192, 0, 2)), newContainerRunRange(0, 16383)},
		{"RunRunDense", newContainerRunRange(0, 8191), newContainerRunRange(4096, 12287)},
		{"RunRunSparseMany", postingBenchSparseRunContainer(1024), postingBenchSparseRunContainer(1024)},
	}
	for i := range pairs {
		left := pairs[i].left
		right := pairs[i].right
		b.Cleanup(func() {
			left.release()
			right.release()
		})
		b.Run("Or/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := left.or(right)
				postingBenchIntSink += out.getCardinality()
				out.release()
			}
		})
		b.Run("And/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := left.and(right)
				postingBenchIntSink += out.getCardinality()
				out.release()
			}
		})
		b.Run("AndCardinality/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchIntSink += left.andCardinality(right)
			}
		})
		b.Run("Intersects/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = left.intersects(right)
			}
		})
		b.Run("Xor/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := left.xor(right)
				postingBenchIntSink += out.getCardinality()
				out.release()
			}
		})
		b.Run("AndNot/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := left.andNot(right)
				postingBenchIntSink += out.getCardinality()
				out.release()
			}
		})
	}
}

func BenchmarkBitmap32SetOps(b *testing.B) {
	type pair struct {
		name        string
		left, right *bitmap32
	}
	pairs := [...]pair{
		{"AllKeysOverlapArray", postingBenchBitmap32FromIDs(buildTestBitmap32IDs(32, 64, 0), false), postingBenchBitmap32FromIDs(buildTestBitmap32IDs(32, 64, 0), false)},
		{"NoKeysOverlap", postingBenchBitmap32FromIDs(buildTestBitmap32IDs(32, 64, 0), false), postingBenchBitmap32FromIDs(buildTestBitmap32IDs(32, 64, 64), false)},
		{"ManyBitmapPayloads", postingBenchBitmap32FromIDs(buildTestBitmap32IDs(8, 4097, 0), false), postingBenchBitmap32FromIDs(buildTestBitmap32IDs(8, 4097, 0), false)},
		{"RunOptimizedDense", postingBenchBitmap32FromIDs(postingBenchBitmap32RangeIDs(8, 8192), true), postingBenchBitmap32FromIDs(postingBenchBitmap32RangeIDs(8, 8192), true)},
	}
	for i := range pairs {
		left := pairs[i].left
		right := pairs[i].right
		b.Cleanup(func() {
			left.release()
			right.release()
		})
		b.Run("And/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := left.clone()
				out.and(right)
				postingBenchUint64Sum += out.cardinality()
				out.release()
			}
		})
		b.Run("AndFresh/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := left.andFresh(right)
				postingBenchUint64Sum += out.cardinality()
				out.release()
			}
		})
		b.Run("AndCardinality/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchUint64Sum += left.andCardinality(right)
			}
		})
		b.Run("Intersects/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = left.intersects(right)
			}
		})
		b.Run("Or/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := left.clone()
				out.or(right)
				postingBenchUint64Sum += out.cardinality()
				out.release()
			}
		})
		b.Run("AndNot/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := left.clone()
				out.andNot(right)
				postingBenchUint64Sum += out.cardinality()
				out.release()
			}
		})
	}
}

func BenchmarkBitmap32Iteration(b *testing.B) {
	arrayContainer := getContainerArrayFromSlice(postingBenchUint16Dense(1024, 0, 2))
	bitmapContainer := postingBenchContainerBitmapFromValues(postingBenchUint16Dense(4097, 0, 2))
	runContainer := newContainerRunRange(0, 4095)
	b.Cleanup(func() {
		arrayContainer.release()
		bitmapContainer.release()
		runContainer.release()
	})
	containers := [...]struct {
		name string
		c    container16
	}{
		{"Array", arrayContainer},
		{"Bitmap", bitmapContainer},
		{"Run", runContainer},
	}
	for _, tc := range containers {
		b.Run("ForEachContainer64/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = forEachContainer64(1<<32, tc.c, postingBenchForEach)
			}
		})
	}
	pairs := [...]struct {
		name        string
		left, right container16
	}{
		{"ArrayArray", arrayContainer, arrayContainer},
		{"ArrayBitmap", arrayContainer, bitmapContainer},
		{"ArrayRun", arrayContainer, runContainer},
		{"BitmapBitmap", bitmapContainer, bitmapContainer},
		{"BitmapRun", bitmapContainer, runContainer},
		{"RunRun", runContainer, runContainer},
	}
	for _, tc := range pairs {
		b.Run("ForEachContainerIntersection64/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = forEachContainerIntersection64(1<<32, tc.left, tc.right, postingBenchForEach)
			}
		})
	}
}

func BenchmarkLargePostingSetOpsShapes(b *testing.B) {
	type pair struct {
		name        string
		left, right List
	}
	leftOverlap := BuildFromSorted(buildTestLargeIDs(16, 64, 0))
	rightOverlap := BuildFromSorted(buildTestLargeIDs(16, 64, 0))
	leftNoKeys := BuildFromSorted(buildTestLargeIDs(16, 64, 0))
	rightNoKeys := BuildFromSorted(buildTestLargeIDs(16, 64, 64))
	leftInterleaved := BuildFromSorted(postingBenchInterleavedLargeIDs(16, 64, false))
	rightInterleaved := BuildFromSorted(postingBenchInterleavedLargeIDs(16, 64, true))
	leftBitmapPayloads := BuildFromSorted(buildTestLargeIDs(4, 4097, 0))
	rightBitmapPayloads := BuildFromSorted(buildTestLargeIDs(4, 4097, 0))
	leftRun := BuildFromSorted(postingBenchRunIDs(8192))
	rightRun := BuildFromSorted(postingBenchRunIDs(8192)[4096:])
	pairs := [...]pair{
		{"AllKeysOverlap", leftOverlap, rightOverlap},
		{"NoKeysOverlap", leftNoKeys, rightNoKeys},
		{"InterleavedKeys", leftInterleaved, rightInterleaved},
		{"ManyBitmapPayloads", leftBitmapPayloads, rightBitmapPayloads},
		{"RunOptimizedDenseRanges", leftRun, rightRun},
	}
	for i := range pairs {
		left := pairs[i].left
		right := pairs[i].right
		b.Cleanup(func() {
			left.Release()
			right.Release()
		})
		lp := left.largeRef()
		rp := right.largeRef()
		b.Run("And/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := lp.clone()
				out.and(rp)
				postingBenchUint64Sum += out.cardinality()
				out.release()
			}
		})
		b.Run("AndCardinality/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchUint64Sum += lp.andCardinality(rp)
			}
		})
		b.Run("Intersects/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = lp.intersects(rp)
			}
		})
		b.Run("Or/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := lp.clone()
				out.or(rp)
				postingBenchUint64Sum += out.cardinality()
				out.release()
			}
		})
		b.Run("AndNot/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := lp.clone()
				out.andNot(rp)
				postingBenchUint64Sum += out.cardinality()
				out.release()
			}
		})
		b.Run("ForEachIntersectingStopFirst/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = lp.forEachIntersecting(rp, postingBenchForEachStop)
			}
		})
		b.Run("ForEachIntersectingConsumeAll/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchBoolSink = lp.forEachIntersecting(rp, postingBenchForEach)
			}
		})
		b.Run("ToArray/"+pairs[i].name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postingBenchSliceSink = lp.toArray()
			}
		})
	}
}

func BenchmarkListBuildSetOpsShapes(b *testing.B) {
	compact := BuildFromSorted(postingBenchMidIDs())
	large := BuildFromSorted(buildTestLargeIDs(16, 64, 0))
	largeDisjoint := BuildFromSorted(buildTestLargeIDs(16, 64, 64))
	denseRunLarge := BuildFromSorted(postingBenchRunIDs(8192))
	compactMask := BuildFromSorted([]uint64{
		1<<32 | 1,
		1<<32 | 3,
		2<<32 | 1,
		2<<32 | 3,
		3<<32 | 1,
		3<<32 | 3,
		4<<32 | 1,
		4<<32 | 3,
		5<<32 | 1,
	})
	defer compact.Release()
	defer large.Release()
	defer largeDisjoint.Release()
	defer denseRunLarge.Release()
	defer compactMask.Release()

	cases := [...]struct {
		name        string
		op          string
		left, right List
		borrowLeft  bool
	}{
		{"BuildAnd/CompactVsLarge", "and", compact, large, false},
		{"BuildAnd/LargeVsCompactOwned", "and", large, compactMask, false},
		{"BuildAnd/LargeVsCompactBorrowed", "and", large, compactMask, true},
		{"BuildAnd/DisjointLarge", "and", large, largeDisjoint, false},
		{"BuildAnd/DenseRunLarge", "and", denseRunLarge, large, false},
		{"BuildOr/CompactVsLarge", "or", compact, large, false},
		{"BuildOr/LargeVsCompactOwned", "or", large, compactMask, false},
		{"BuildOr/LargeVsCompactBorrowed", "or", large, compactMask, true},
		{"BuildOr/DisjointLarge", "or", large, largeDisjoint, false},
		{"BuildOr/DenseRunLarge", "or", denseRunLarge, large, false},
		{"BuildAndNot/CompactVsLarge", "andnot", compact, large, false},
		{"BuildAndNot/LargeVsCompactOwned", "andnot", large, compactMask, false},
		{"BuildAndNot/LargeVsCompactBorrowed", "andnot", large, compactMask, true},
		{"BuildAndNot/DisjointLarge", "andnot", large, largeDisjoint, false},
		{"BuildAndNot/DenseRunLarge", "andnot", denseRunLarge, large, false},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var left List
				if tc.borrowLeft {
					left = tc.left.Borrow()
				} else {
					left = tc.left.Clone()
				}
				var out List
				switch tc.op {
				case "and":
					out = left.BuildAnd(tc.right)
				case "or":
					out = left.BuildOr(tc.right)
				default:
					out = left.BuildAndNot(tc.right)
				}
				postingBenchUint64Sum += out.Cardinality()
				out.Release()
			}
		})
	}
}

func BenchmarkListBuildAddedManyShapes(b *testing.B) {
	base := BuildFromSorted(buildTestLargeIDs(16, 64, 0))
	sortedAppend := buildTestLargeIDs(4, 32, 32)
	sortedInterleaved := buildTestLargeIDs(4, 32, 4)
	groupedUnsorted := []uint64{
		40<<32 | 9, 40<<32 | 1, 40<<32 | 5,
		41<<32 | 7, 41<<32 | 3, 41<<32 | 11,
	}
	ungroupedUnsorted := []uint64{
		40<<32 | 9, 41<<32 | 7, 40<<32 | 1,
		41<<32 | 3, 40<<32 | 5, 41<<32 | 11,
	}
	duplicates := []uint64{
		40<<32 | 1, 40<<32 | 1, 40<<32 | 3, 40<<32 | 3,
	}
	rangeBatch := postingBenchRunIDs(256)
	midBase := BuildFromSorted(postingBenchMidIDs())
	defer base.Release()
	defer midBase.Release()
	cases := [...]struct {
		name string
		base List
		add  []uint64
	}{
		{"SortedAppendAfterMaxHighbits", base, sortedAppend},
		{"SortedInterleaved", base, sortedInterleaved},
		{"GroupedUnsorted", base, groupedUnsorted},
		{"UngroupedUnsorted", base, ungroupedUnsorted},
		{"Duplicates", base, duplicates},
		{"RangeShapedBatch", base, rangeBatch},
		{"MidCapToLarge", midBase, sortedAppend},
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

func BenchmarkListBuildOptimizedShapes(b *testing.B) {
	largeToSmall := postingBenchLargeListFromSorted(postingBenchSmallIDs())
	largeToMid := postingBenchLargeListFromSorted(postingBenchMidIDs())
	largeStaysLarge := BuildFromSorted(buildTestLargeIDs(16, 64, 0))
	largeRunOptimizes := BuildFromSorted(postingBenchRunIDs(8192))
	defer largeToSmall.Release()
	defer largeToMid.Release()
	defer largeStaysLarge.Release()
	defer largeRunOptimizes.Release()
	cases := [...]struct {
		name     string
		base     List
		borrowed bool
	}{
		{"LargeToSmall", largeToSmall, false},
		{"LargeToMid", largeToMid, false},
		{"LargeStaysLarge", largeStaysLarge, false},
		{"LargeRunOptimizes", largeRunOptimizes, false},
		{"BorrowedLargeToSmall", largeToSmall, true},
		{"BorrowedLargeToMid", largeToMid, true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var out List
				if tc.borrowed {
					out = tc.base.Borrow()
				} else {
					out = tc.base.Clone()
				}
				out = out.BuildOptimized()
				postingBenchUint64Sum += out.Cardinality()
				out.Release()
			}
		})
	}
}

func BenchmarkListTryResetOwnedLargeFromSortedShapes(b *testing.B) {
	large := BuildFromSorted(buildTestLargeIDs(16, 64, 0))
	defer large.Release()
	cases := [...]struct {
		name string
		ids  []uint64
	}{
		{"Empty", nil},
		{"CompactSized", postingBenchMidIDs()},
		{"Range", postingBenchRunIDs(256)},
		{"SparseMultiHighbits", buildTestLargeIDs(8, 32, 32)},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			scratch := large.Clone()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var ok bool
				scratch, ok = scratch.TryResetOwnedLargeFromSorted(tc.ids)
				postingBenchBoolSink = ok
				postingBenchUint64Sum += scratch.Cardinality()
				if scratch.IsEmpty() {
					scratch = large.Clone()
				}
			}
			b.StopTimer()
			scratch.Release()
		})
	}
}
