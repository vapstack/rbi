package indexdata

import (
	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const (
	fieldIndexSingleChunkCap       = 32768
	fieldIndexSingleAdaptiveMaxLen = 200_000
	fieldIndexSingleInlineCap      = 16
)

type fieldIndexPostingUnionBuilder struct {
	ids           posting.List
	singles       []uint64
	inlineSingles [fieldIndexSingleInlineCap]uint64
	inlineLen     int
	batchSingles  bool
}

func fieldIndexPostingBatchSinglesEnabled(capHint uint64) bool {
	return capHint > posting.SmallCap && capHint <= fieldIndexSingleAdaptiveMaxLen
}

func newFieldIndexPostingUnionBuilder(batchSingles bool) fieldIndexPostingUnionBuilder {
	return fieldIndexPostingUnionBuilder{batchSingles: batchSingles}
}

func (b *fieldIndexPostingUnionBuilder) flushSingles() {
	if b.singles != nil {
		if len(b.singles) == 0 {
			return
		}
		b.ids = b.ids.BuildAddedMany(b.singles)
		b.singles = b.singles[:0]
		return
	}
	if b.inlineLen == 0 {
		return
	}
	b.ids = b.ids.BuildAddedMany(b.inlineSingles[:b.inlineLen])
	b.inlineLen = 0
}

func (b *fieldIndexPostingUnionBuilder) addSingle(idx uint64) {
	if !b.batchSingles {
		b.ids = b.ids.BuildAdded(idx)
		return
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
	if b.batchSingles {
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
	b.flushSingles()
	if b.ids.IsEmpty() {
		b.ids = ids.Borrow()
		return
	}
	b.ids = b.ids.BuildOr(ids)
}

func (b *fieldIndexPostingUnionBuilder) finish(optimize bool) posting.List {
	b.flushSingles()
	out := b.ids
	if out.IsBorrowed() {
		out = out.Clone()
	}
	b.ids = posting.List{}
	b.inlineLen = 0
	if b.singles != nil {
		pooled.ReleaseUint64Slice(b.singles)
		b.singles = nil
	}
	if optimize {
		return out.BuildOptimized()
	}
	return out
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

func (o FieldIndexView) unionRangePostingsBatchSinglesEnabled(first, second FieldIndexRange) bool {
	totalSpan := first.Len() + second.Len()
	if totalSpan == 0 {
		return false
	}
	if totalSpan <= fieldIndexSingleAdaptiveMaxLen {
		return true
	}
	_, estFirst := o.RangeStats(first)
	_, estSecond := o.RangeStats(second)

	return fieldIndexPostingBatchSinglesEnabled(satAddUint64(estFirst, estSecond))
}

func satAddUint64(total, add uint64) uint64 {
	if ^uint64(0)-total < add {
		return ^uint64(0)
	}
	return total + add
}

func (o FieldIndexView) UnionRangePostings(first, second FieldIndexRange) posting.List {
	if first.Empty() && second.Empty() {
		return posting.List{}
	}
	builder := newFieldIndexPostingUnionBuilder(o.unionRangePostingsBatchSinglesEnabled(first, second))
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

	builder := newFieldIndexPostingUnionBuilder(totalSpan <= fieldIndexSingleAdaptiveMaxLen)
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
