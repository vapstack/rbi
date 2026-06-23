package indexdata

import (
	"math/bits"
	"slices"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/mathutil"
	"github.com/vapstack/rbi/internal/posting"
)

const (
	fieldIndexSingleChunkCap                 = 32768
	fieldIndexCompactAsSinglesAdaptiveMaxLen = 200_000
	fieldIndexSingleInlineCap                = 16
)

type fieldIndexPostingUnionBuilder struct {
	ids             posting.List
	singleIDs       posting.List
	singles         []uint64
	inlineSingles   [fieldIndexSingleInlineCap]uint64
	inlineLen       int
	lastSingle      uint64
	maxSingle       uint64
	singlesUnsorted bool
	batchCompact    bool
}

func fieldIndexPostingBatchCompactEnabled(capHint uint64) bool {
	return capHint > posting.SmallCap && capHint <= fieldIndexCompactAsSinglesAdaptiveMaxLen
}

func newFieldIndexPostingUnionBuilder(batchCompact bool) fieldIndexPostingUnionBuilder {
	return fieldIndexPostingUnionBuilder{batchCompact: batchCompact}
}

func (b *fieldIndexPostingUnionBuilder) flushSingles() {
	if b.singles != nil {
		if len(b.singles) == 0 {
			return
		}
		if b.singlesUnsorted {
			sortFieldIndexSingles(b.singles, b.maxSingle)
		}
		b.singleIDs = b.singleIDs.BuildAddedMany(b.singles)
		b.singles = b.singles[:0]
		b.lastSingle = 0
		b.maxSingle = 0
		b.singlesUnsorted = false

	} else {
		if b.inlineLen == 0 {
			return
		}
		singles := b.inlineSingles[:b.inlineLen]
		if b.singlesUnsorted {
			sortFieldIndexSingles(singles, b.maxSingle)
		}
		b.singleIDs = b.singleIDs.BuildAddedMany(singles)
		b.inlineLen = 0
		b.lastSingle = 0
		b.maxSingle = 0
		b.singlesUnsorted = false
	}
}

func (b *fieldIndexPostingUnionBuilder) addSingle(idx uint64) {
	if b.inlineLen != 0 || (b.singles != nil && len(b.singles) != 0) {
		b.singlesUnsorted = b.singlesUnsorted || idx < b.lastSingle
	}
	b.lastSingle = idx
	if idx > b.maxSingle {
		b.maxSingle = idx
	}

	if b.singles == nil {
		if b.inlineLen < len(b.inlineSingles) {
			b.inlineSingles[b.inlineLen] = idx
			b.inlineLen++
			return
		}
		b.singles = pooled.GetUint64Slice(fieldIndexSingleChunkCap)
		b.singles = append(b.singles, b.inlineSingles[:]...)
		b.inlineLen = 0
	}
	b.singles = append(b.singles, idx)
	if len(b.singles) == cap(b.singles) {
		b.flushSingles()
	}
}

func (b *fieldIndexPostingUnionBuilder) addPosting(ids posting.List) {
	if ids.IsEmpty() {
		return
	}
	if b.batchCompact {
		var compact [posting.MidCap]uint64
		if values, ok := ids.TryAppendCompactTo(compact[:0]); ok {
			for _, idx := range values {
				b.addSingle(idx)
			}
			return
		}
	}
	if idx, ok := ids.TrySingle(); ok {
		b.addSingle(idx)
		return
	}
	if b.ids.IsEmpty() {
		b.ids = ids.Borrow()
		return
	}
	b.ids = b.ids.BuildOr(ids)
}

func (b *fieldIndexPostingUnionBuilder) finish(optimize bool) posting.List {
	b.flushSingles()
	out := b.ids
	if !b.singleIDs.IsEmpty() {
		singleIDs := b.singleIDs
		b.singleIDs = posting.List{}
		if out.IsEmpty() {
			out = singleIDs
		} else {
			out = out.BuildOr(singleIDs)
			singleIDs.Release()
		}
	}
	if out.IsBorrowed() {
		out = out.Clone()
	}
	b.ids = posting.List{}
	b.inlineLen = 0
	b.lastSingle = 0
	b.maxSingle = 0
	b.singlesUnsorted = false
	if b.singles != nil {
		pooled.ReleaseUint64Slice(b.singles)
		b.singles = nil
	}
	if optimize {
		return out.BuildOptimized()
	}
	return out
}

func sortFieldIndexSingles(ids []uint64, maxID uint64) {
	if len(ids) < 1024 {
		slices.Sort(ids)
		return
	}
	passes := (bits.Len64(maxID) + 7) >> 3
	if passes == 0 {
		return
	}
	buf := pooled.GetUint64Slice(len(ids))[:len(ids)]
	src := ids
	dst := buf

	for pass := 0; pass < passes; pass++ {
		var offsets [256]int
		shift := uint(pass << 3)
		for i := 0; i < len(src); i++ {
			offsets[byte(src[i]>>shift)]++
		}
		sum := 0
		for i := 0; i < len(offsets); i++ {
			count := offsets[i]
			offsets[i] = sum
			sum += count
		}
		for i := 0; i < len(src); i++ {
			v := src[i]
			bucket := byte(v >> shift)
			dst[offsets[bucket]] = v
			offsets[bucket]++
		}
		src, dst = dst, src
	}
	if passes&1 != 0 {
		copy(ids, src)
	}
	pooled.ReleaseUint64Slice(buf)
}

func (o FieldIndexView) appendRangePostingsUnion(builder *fieldIndexPostingUnionBuilder, br FieldIndexRange) {
	if br.Empty() {
		return
	}
	cur := o.NewCursor(br, false)
	for {
		ids, idx, single, ok := cur.NextPostingOrSingle()
		if !ok {
			break
		}
		if single {
			builder.addSingle(idx)
			continue
		}
		if ids.IsEmpty() {
			continue
		}
		builder.addPosting(ids)
	}
}

func (o FieldIndexView) unionRangePostingsBatchCompactEnabled(first, second FieldIndexRange) bool {
	totalSpan := first.Len() + second.Len()
	if totalSpan == 0 {
		return false
	}
	if totalSpan <= fieldIndexCompactAsSinglesAdaptiveMaxLen {
		return true
	}
	_, estFirst := o.RangeStats(first)
	_, estSecond := o.RangeStats(second)

	return fieldIndexPostingBatchCompactEnabled(mathutil.SatAddUint64(estFirst, estSecond))
}

func (o FieldIndexView) mergeRangePostingsBatchCompactEnabled(first, second FieldIndexRange, totalSpan int) bool {
	if totalSpan <= fieldIndexCompactAsSinglesAdaptiveMaxLen {
		return true
	}
	if first.filterPrefixSet || second.filterPrefixSet {
		return false
	}
	bucketsFirst, rowsFirst := o.RangeStats(first)
	bucketsSecond, rowsSecond := o.RangeStats(second)
	buckets := bucketsFirst + bucketsSecond
	if buckets == 0 {
		return false
	}
	rows := mathutil.SatAddUint64(rowsFirst, rowsSecond)
	if rows <= posting.SmallCap {
		return false
	}
	return rows <= uint64(buckets)*posting.MidCap
}

func (o FieldIndexView) UnionRangePostings(first, second FieldIndexRange) posting.List {
	if first.Empty() && second.Empty() {
		return posting.List{}
	}
	builder := newFieldIndexPostingUnionBuilder(o.unionRangePostingsBatchCompactEnabled(first, second))
	o.appendRangePostingsUnion(&builder, first)
	o.appendRangePostingsUnion(&builder, second)

	return builder.finish(true)
}

func (o FieldIndexView) MergeRangePostingsInto(dst posting.List, first, second FieldIndexRange) posting.List {
	totalSpan := first.Len() + second.Len()
	if totalSpan == 0 {
		return dst
	}
	const mergeFieldIndexRangeDirectMaxBuckets = 32

	builder := newFieldIndexPostingUnionBuilder(o.mergeRangePostingsBatchCompactEnabled(first, second, totalSpan))
	if !dst.IsEmpty() && totalSpan > mergeFieldIndexRangeDirectMaxBuckets {
		o.appendRangePostingsUnion(&builder, first)
		o.appendRangePostingsUnion(&builder, second)
		return dst.BuildMergedOwned(builder.finish(false))
	}
	builder.ids = dst

	o.appendRangePostingsUnion(&builder, first)
	o.appendRangePostingsUnion(&builder, second)

	return builder.finish(false)
}
